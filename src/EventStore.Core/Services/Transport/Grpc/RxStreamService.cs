using System;
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
using Grpc.Core;
using QbservableProvider.Core;
using Qube.Core;
using Qube.Grpc;
using Event = Qube.EventStore.Event;
using Qube.Grpc.Utils;

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
			IServerStreamWriter<ResponseEnvelope> responseStream,
			ServerCallContext context
		) {
			var subject = new Subject<Event>();
			ServerQueryObservable<Event, object> qbservable;

			try {
				qbservable = BuildQbservable(queryEnvelope, subject);
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error building qbservable");
				await ClientOnError(responseStream, ex);
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
				async e => { await ClientOnNext(responseStream, e); },
				async ex => { await ClientOnError(responseStream, ex); done = true; },
				async () => { await ClientOnCompleted(responseStream); done = true; })
			) {
				GetNextBatch(position);

				// Waiting for completion this way isn't ideal but gRpc has an issue running inside
				// another thread via Task.Run or qbservable.RunAsync(): 'Response stream is already completed'
				while (!done) {
					await Task.Delay(25);
				}
			}
		}

		private static ServerQueryObservable<Event, object> BuildQbservable(QueryEnvelope queryEnvelope, Subject<Event> subject) {
			var queryExpression = SerializationHelper.DeserializeLinqExpression(queryEnvelope.Payload);
			var qbservable = new ServerQueryObservable<Event, object>(subject.AsQbservable(), queryExpression);
			return qbservable;
		}

		private async Task ClientOnNext(
			IServerStreamWriter<ResponseEnvelope> responseStream,
			object payload
		) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = EnvelopeHelper.Pack(payload),
				ResponseType = ResponseEnvelope.Types.ResponseType.Next
			});
		}

		private async Task ClientOnError(
			IServerStreamWriter<ResponseEnvelope> responseStream,
			Exception ex
		) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = EnvelopeHelper.Pack(ex),
				ResponseType = ResponseEnvelope.Types.ResponseType.Error
			});
		}

		private async Task ClientOnCompleted(IServerStreamWriter<ResponseEnvelope> responseStream) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = "",
				ResponseType = ResponseEnvelope.Types.ResponseType.Completed
			});
		}

		private async Task SendEnvelopeToClient(IServerStreamWriter<ResponseEnvelope> responseStream, ResponseEnvelope ResponseEnvelope) {
			// gRpc - only one write can be pending at a time.
			await _writeLock.WaitAsync();

			try {
				await responseStream.WriteAsync(ResponseEnvelope);
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
