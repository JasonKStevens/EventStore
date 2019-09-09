using System;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Data;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Core.Services.UserManagement;
using Grpc.Core;
using Newtonsoft.Json;
using Qube.Grpc;
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
			ServerCallContext callContext
		) {
			var streamPatterns = JsonConvert.DeserializeObject<string[]>(queryEnvelope.StreamPattern);

			try {
				var broker = new GrpcBroker(queryEnvelope);
				await Run(broker, responseStream, streamPatterns);
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error running Rx query");
				await ClientOnError(responseStream, ex);
			}
		}

		private async Task Run(GrpcBroker broker, IServerStreamWriter<ResponseEnvelope> responseStream, string[] streamPatterns) {

			var done = false;
			var position = new TFPos();
			var operationId = Guid.NewGuid();
			RxSubjectEnvelope envelope = null;

			void GetNextBatch(TFPos pos) {
				envelope = envelope ?? new RxSubjectEnvelope(
					broker.SourceType,
					broker.RegisteredTypes,
					broker.Observer,
					streamPatterns,
					GetNextBatch);
				var message = BuildMessage(operationId, envelope, pos);
				_publisher.Publish(message);
			}

			using (var sub = broker.Observable.Subscribe(
				async e => { try { await ClientOnNext(responseStream, e); } catch { done = true; } },
				async ex => { await ClientOnError(responseStream, ex); done = true; },
				async () => { await ClientOnCompleted(responseStream); done = true; })
			) {
				GetNextBatch(position);

				// TODO: Investiate the following
				// Waiting for completion this way isn't good but gRpc has an issue running inside
				// another thread via Task.Run or qbservable.RunAsync(): 'Response stream is already completed'
				while (!done) {
					await Task.Delay(25);
				}
			}
		}

		private async Task ClientOnNext(
			IServerStreamWriter<ResponseEnvelope> responseStream,
			object payload
		) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = EnvelopeHelper.Pack(payload),
				PayloadType = payload.GetType().FullName,
				RxMethod = ResponseEnvelope.Types.RxMethod.Next
			});
		}

		private async Task ClientOnError(
			IServerStreamWriter<ResponseEnvelope> responseStream,
			Exception ex
		) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = EnvelopeHelper.Pack(ex),
				PayloadType = ex.GetType().FullName,
				RxMethod = ResponseEnvelope.Types.RxMethod.Error
			});
		}

		private async Task ClientOnCompleted(IServerStreamWriter<ResponseEnvelope> responseStream) {
			await SendEnvelopeToClient(responseStream, new ResponseEnvelope {
				Payload = "",
				PayloadType = "",
				RxMethod = ResponseEnvelope.Types.RxMethod.Completed
			});
		}

		private async Task SendEnvelopeToClient(IServerStreamWriter<ResponseEnvelope> responseStream, ResponseEnvelope responseEnvelope) {
			// gRpc - only one write can be pending at a time.
			await _writeLock.WaitAsync();

			try {
				await responseStream.WriteAsync(responseEnvelope);
			} catch (Exception ex) {
				Log.ErrorException(ex, "Error writing to gRpc response stream");
				throw;
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
