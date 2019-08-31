using System;
using System.Collections.Generic;
using System.Text;
using System.Reactive.Subjects;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using static EventStore.Core.Messages.ClientMessage;
using Newtonsoft.Json;
using System.Linq;

namespace EventStore.Core.Services.Transport.Grpc {
	public class RxSubjectEnvelope : IEnvelope {
		private readonly Subject<object> _subject;
		private readonly Action<TFPos> _getNextBatch;
		private readonly Dictionary<Type, Action<object>> _messageHandlers;

		public RxSubjectEnvelope(Type sourceType, Subject<object> subject, Action<TFPos> nextBatch) {
			_subject = subject;
			_getNextBatch = nextBatch;

			_messageHandlers = new Dictionary<Type, Action<object>> {
				{ typeof(ReadAllEventsForwardCompleted), m => OnMessage(sourceType, (ReadAllEventsForwardCompleted) m) },
			};
		}

		public void ReplyWith<T>(T message) where T : Message {
			if (_messageHandlers.ContainsKey(typeof(T))) {
				_messageHandlers[typeof(T)](message);
			}
		}

		private void OnMessage(Type sourceType, ReadAllEventsForwardCompleted message) {
			var events = message.Events
				.Where(e => e.OriginalEvent.EventType == sourceType.Name)
				.Select(e => Encoding.UTF8.GetString(e.OriginalEvent.Data))
				.Select(s => JsonConvert.DeserializeObject(s, sourceType));

			foreach (var @event in events) {
				_subject.OnNext(@event);
			}

			if (message.IsEndOfStream) {
				_subject.OnCompleted();
			} else  if (message.Error != null) {
				_subject.OnError(new Exception(message.Error));
			} else {
				_getNextBatch(message.NextPos);
			}
		}
	}
}
