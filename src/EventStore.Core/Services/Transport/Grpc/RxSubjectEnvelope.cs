using System;
using System.Collections.Generic;
using System.Text;
using System.Reactive.Subjects;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using static EventStore.Core.Messages.ClientMessage;
using Event = Qube.EventStore.Event;

namespace EventStore.Core.Services.Transport.Grpc {
	public class RxSubjectEnvelope : IEnvelope {
		private readonly Subject<Event> _subject;
		private readonly Action<TFPos> _getNextBatch;
		private readonly Dictionary<Type, Action<object>> _messageHandlers;

		public RxSubjectEnvelope(Subject<Event> subject, Action<TFPos> nextBatch) {
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
				var Event = ToEventData(recordedEvent.OriginalEvent);
				_subject.OnNext(Event);
			}

			if (message.IsEndOfStream) {
				_subject.OnCompleted();
			} else  if (message.Error != null) {
				_subject.OnError(new Exception(message.Error));
			} else {
				_getNextBatch(message.NextPos);
			}
		}

		private Event ToEventData(EventRecord @event) {

			return new Qube.EventStore.Event {
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
