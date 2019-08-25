using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Text;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using EventStore.Transport.Grpc;
using static EventStore.Core.Messages.ClientMessage;

namespace EventStore.Core.Services.Transport.Grpc {
	public class RxSubjectEnvelope : IEnvelope {
		private readonly Subject<GrpcEvent> _subject;
		private readonly Action<TFPos> _getNextBatch;
		private readonly Dictionary<Type, Action<object>> _messageHandlers;

		public RxSubjectEnvelope(Subject<GrpcEvent> subject, Action<TFPos> nextBatch) {
			_subject = subject;
			_getNextBatch = nextBatch;

			_messageHandlers = new Dictionary<Type, Action<object>> {
				{ typeof(ReadAllEventsForwardCompleted), m => OnMessage((ReadAllEventsForwardCompleted) m) },
			};
		}

		public void ReplyWith<T>(T message) where T : Message {
			if (_messageHandlers.ContainsKey(typeof(T))) {
				_messageHandlers[typeof(T)](message);
			}
		}

		private void OnMessage(ReadAllEventsForwardCompleted message) {
			foreach (var recordedEvent in message.Events) {
				var grpcEvent = ToEventData(recordedEvent.OriginalEvent);
				_subject.OnNext(grpcEvent);
			}

			if (message.IsEndOfStream) {
				_subject.OnCompleted();
			} else  if (message.Error != null) {
				_subject.OnError(new Exception(message.Error));
			} else {
				_getNextBatch(message.NextPos);
			}
		}

		private GrpcEvent ToEventData(EventRecord @event) {

			return new GrpcEvent {
				EventStreamId = @event.EventStreamId,
				EventId = @event.EventId,
				EventNumber = @event.EventNumber,
				EventType = @event.EventType,
				IsJson = @event.IsJson,
				Data = Encoding.UTF8.GetString(@event.Data),
				Metadata = Encoding.UTF8.GetString(@event.Metadata),
				Created = @event.TimeStamp,
			};
		}
	}
}
