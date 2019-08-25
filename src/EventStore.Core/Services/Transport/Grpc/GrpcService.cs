using System;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using Grpc.Core;
using QbservableProvider.EventStore;

namespace EventStore.Core.Services.Transport.Grpc {
	public class GrpcService :
		IHandle<SystemMessage.SystemInit>,
		IHandle<SystemMessage.SystemStart>,
		IHandle<SystemMessage.BecomeShuttingDown>
	{
		private static readonly ILogger Log = LogManager.GetLoggerFor<ClusterVNode>();

		private readonly IPublisher _publisher;
		private readonly ServerPort _serverPort;
		private Server _server;

		public GrpcService(
			IPublisher publisher,
			string host,
			int port
		) {
			_publisher = publisher;
			_serverPort = new ServerPort(host, port, ServerCredentials.Insecure);
		}

		public void Handle(SystemMessage.SystemInit message) {
			_server = new Server {
				Services = { StreamService.BindService(new RxStreamService(_publisher)) },
				Ports = { _serverPort }
			};
			Log.Info("Initialized gRpc server");
		}

		public void Handle(SystemMessage.SystemStart message) {
			try {
				_server.Start();
				Log.Info("Started gRpc server to run on host {0} port {1}", _serverPort.Host, _serverPort.Port);
			} catch (Exception e) {
				Log.ErrorException(e, "Error starting gRpc server on host {0} port {1}", _serverPort.Host, _serverPort.Port);
			}
		}

		public void Handle(SystemMessage.BecomeShuttingDown message) {
			_server.ShutdownAsync()
				.ContinueWith(t => {
					if (t.IsFaulted) {
						Log.ErrorException(t.Exception, "Error shutting down gRpc server");
					}
				});
		}
	}
}
