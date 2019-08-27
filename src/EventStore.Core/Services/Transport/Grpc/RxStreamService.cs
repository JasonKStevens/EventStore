using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reflection;
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
using Qube.EventStore;

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

				// Waiting for completion this way isn't ideal but gRpc has an issue running inside
				// another thread via Task.Run or qbservable.RunAsync(): 'Response stream is already completed'
				while (!done) {
					await Task.Delay(25);
				}
			}
		}

		private async Task<IQbservable<object>> BuildQbservableAsync(
			string serializedRxQuery,
			Subject<GrpcEvent> subject,
			IServerStreamWriter<EventEnvelope> responseStream
		) {
			try {
				var expression = SerializationHelper.DeserializeLinqExpression(serializedRxQuery);
				var lambdaExpression = (LambdaExpression)expression;

				var castLambdaExpression = CastGenericItemToObject(lambdaExpression);

				var qbservable = castLambdaExpression
					.Compile()
					.DynamicInvoke(subject.AsQbservable());

				return (IQbservable<object>) qbservable;
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error deserializing Qbservable query");
				await SendErrorToClient(responseStream, "Unsupported linq expression: " + ex.Message);
				return null;
			}
		}

		/// <summary>
		/// Call .Cast<object>() Rx querie's lambda expression to cast result type to "known" object.
		/// </summary>
		private static LambdaExpression CastGenericItemToObject(LambdaExpression lambdaExpression) {
			var castMethod = typeof(Qbservable)
				.GetMethods(BindingFlags.Static | BindingFlags.Public)
				.Where(mi => mi.Name == "Cast")
				.Single()
				.MakeGenericMethod(typeof(object));

			var methodCall = Expression.Call(null, castMethod, lambdaExpression.Body);
			var castLambdaExpression = Expression.Lambda(methodCall, lambdaExpression.Parameters);

			return castLambdaExpression;
		}

		private async Task SendNextToClient(
			IServerStreamWriter<EventEnvelope> responseStream,
			object item
		) {
			var eventEnvelope = SerializationHelper.Pack(item);
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
