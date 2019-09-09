using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using EventStore.Core.Data;
using EventStore.Core.Messaging;
using static EventStore.Core.Messages.ClientMessage;

namespace EventStore.Core.Services.Transport.Grpc {
	public class RxSubjectEnvelope : IEnvelope {
		private readonly IObserver<object> _observer;
		private readonly Action<TFPos> _getNextBatch;
		private readonly string[] _streamPatterns;
		private readonly Dictionary<Type, Action<object>> _messageHandlers;

		public RxSubjectEnvelope(
			Type sourceType,
			Type[] regTypes,
			IObserver<object> subject,
			string[] streamPatterns,
			Action<TFPos> nextBatch
		) {
			_observer = subject;
			_getNextBatch = nextBatch;
			_streamPatterns = streamPatterns.Select(WildCardToRegularExpression).ToArray();

			_messageHandlers = new Dictionary<Type, Action<object>> {
				{ typeof(ReadAllEventsForwardCompleted), m => OnMessage(sourceType, regTypes, (ReadAllEventsForwardCompleted) m) },
			};
		}

		public void ReplyWith<T>(T message) where T : Message {
			if (_messageHandlers.ContainsKey(typeof(T))) {
				_messageHandlers[typeof(T)](message);
			}
		}

		private void OnMessage(Type sourceType, Type[] registeredTypes, ReadAllEventsForwardCompleted message) {
			bool NotSystemEvent(ResolvedEvent @event) => !@event.OriginalEvent.EventType.StartsWith("$");
			bool IsRegisteredType(ResolvedEvent @event) => registeredTypes.Any(t => t.Name == @event.OriginalEvent.EventType);
			bool MatchesStreamPatterns(ResolvedEvent @event) => _streamPatterns.Length == 0 || _streamPatterns.Any(p => Regex.IsMatch(@event.OriginalStreamId, p, RegexOptions.IgnoreCase));

			var events = message.Events
				.Where(NotSystemEvent)
				.Where(MatchesStreamPatterns)
				.Where(IsRegisteredType)
				//.Where(e => {
				//	var eventType = registeredTypes.FirstOrDefault(t => t.FullName == e.OriginalEvent.EventType);
				//	if (eventType == null) {
				//		return false;
				//	}
				//	return eventType.IsAssignableFrom(sourceType);
				//})
				.Select(e => {
					var str = Encoding.UTF8.GetString(e.OriginalEvent.Data);
					var type = registeredTypes.FirstOrDefault(t => t.Name == e.OriginalEvent.EventType) ?? typeof(object);

					var evt = JsonConvert.DeserializeObject(str, type);

					var bagProperty = type.GetProperty("Bag");
					if (bagProperty != null) {
						// TEMP:: Experiment
						type.GetProperty("Bag").SetValue(evt, Activator.CreateInstance(typeof(Dictionary<string, object>)));
					}

					return evt;
				});

			foreach (var @event in events) {
				_observer.OnNext(@event);
			}

			if (message.IsEndOfStream) {
				_observer.OnCompleted();
			} else  if (message.Error != null) {
				_observer.OnError(new Exception(message.Error));
			} else {
				_getNextBatch(message.NextPos);
			}
		}

		private static string WildCardToRegularExpression(string value) {
			return "^" + Regex.Escape(value).Replace("\\*", ".*") + "$";
		}
	}
}
