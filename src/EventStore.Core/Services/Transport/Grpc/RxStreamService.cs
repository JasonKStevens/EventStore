using System;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.UserManagement;
using EventStore.Transport.Grpc;
using EventStore.Transport.Grpc.Utils;
using Grpc.Core;
using QbservableProvider.EventStore;

namespace EventStore.Core.Services.Transport.Grpc {
	public class RxStreamService : StreamService.StreamServiceBase {
		private static readonly ILogger Log = LogManager.GetLoggerFor<ClusterVNode>();

		private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);
		private readonly IPublisher _publisher;

		public RxStreamService(IPublisher publisher) {
			_publisher = publisher;
		}

		public override async Task QueryStreamAsync(
			QueryEnvelope queryEnvelope,
			IServerStreamWriter<EventEnvelope> responseStream,
			ServerCallContext context
		) {
			var subject = new Subject<GrpcEvent>();
			var qbservable = await BuildQbservableAsync(queryEnvelope.Payload, subject, responseStream);

			if (qbservable == null) {
				return;
			}

			var done = false;
			var position = new TFPos();
			var operationId = Guid.NewGuid();
			RxSubjectEnvelope envelope = null;

			void GetNextBatch(TFPos pos) {
				envelope = envelope ?? new RxSubjectEnvelope(subject, GetNextBatch);
				var message = BuildMessage(operationId, envelope, pos);
				_publisher.Publish(message);
			}

			using (var sub = qbservable.Subscribe(
				async e => { await SendNextToClient(responseStream, e); },
				async ex => { await SendErrorToClient(responseStream, ex.Message); done = true; },
				() => { /* No need to send completed to client */ done = true; })
			) {
				GetNextBatch(position);

				// Waiting for completion this way is unfortunate but gRpc seems to have an issue running
				// inside of other thread via Task.Run or qbservable.RunAsync(): 'Response stream is already completed'
				while (!done) {
					await Task.Delay(25);
				}
			}
		}

		private async Task<IQbservable<GrpcEvent>> BuildQbservableAsync(
			string serializedRxQuery,
			Subject<GrpcEvent> subject,
			IServerStreamWriter<EventEnvelope> responseStream) {
			try {
				var expression = SerializationHelper.DeserializeLinqExpression(serializedRxQuery);
				var lambdaExpr = (LambdaExpression)expression;
				var lambda = lambdaExpr.Compile();

				var qbservable = (IQbservable<GrpcEvent>)lambda.DynamicInvoke(subject.AsQbservable());
				return qbservable;
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error deserializing Qbservable query");
				await SendErrorToClient(responseStream, "Unsupported linq expression: " + ex.Message);
				return null;
			}
		}

		private async Task SendNextToClient(
			IServerStreamWriter<EventEnvelope> responseStream,
			GrpcEvent grpcEvent
		) {
			var eventEnvelope = SerializationHelper.Pack(grpcEvent);
			await SendEnvelopeToClient(responseStream, eventEnvelope);
		}

		private async Task SendErrorToClient(
			IServerStreamWriter<EventEnvelope> responseStream,
			string errorMessage
		) {
			var eventEnvelope = new EventEnvelope { Error = errorMessage };
			await SendEnvelopeToClient(responseStream, eventEnvelope);
		}

		private async Task SendEnvelopeToClient(IServerStreamWriter<EventEnvelope> responseStream, EventEnvelope eventEnvelope) {
			// TODO: Consider using a buffer - only one write can be pending at a time.
			await _writeLock.WaitAsync();

			try {
				await responseStream.WriteAsync(eventEnvelope);
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error writing to gRpc response stream");
			} finally {
				_writeLock.Release();
			}
		}

		private Message BuildMessage(Guid operationId, IEnvelope envelope, TFPos position) {
			var message = new ClientMessage.ReadAllEventsForward(
				internalCorrId: operationId,
				correlationId: operationId,
				envelope,
				commitPosition: position.CommitPosition,
				preparePosition: position.PreparePosition,
				maxCount: 100,
				resolveLinkTos: false,
				requireMaster: false,
				validationTfLastCommitPosition: null,
				user: SystemAccount.Principal
			);
			return message;
		}
	}
}
