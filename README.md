# Event Store

This is an experimental fork of [EventStore](https://github.com/EventStore/EventStore) which includes support for Reactive Extension (Rx) linq queries via an IQbservableProvider.

There's an [Rx client](https://github.com/JasonKStevens/QbservableProvider) for this, although I haven't pushed the latest changes yet. This should hopefully be in the next week or so (just a bit of tidy up required after a spike).

```c#
var options = new StreamDbContextOptionsBuilder()
    .UseEventStore("127.0.0.1:5001")  // TODO: support ES connection string format
    .Options;

new EventStoreContext(options)
    .FromAll()
    .Where(e => e.EventType == "CustomerCreatedEvent")
    .Where(e => new DateTime(2018, 10, 1) <= e.Created && e.Created < new DateTime(2018, 11, 1))
    .Where(e => e.Data.Contains(".test@somewhere.co.nz"))
    .Subscribe(
        onNext: s =>
        {
            var @event = JsonConvert.DeserializeObject<CustomerCreatedEvent>(s.Data);
            Console.WriteLine($"{@event.CustomerId}: {@event.Email}");
                },
        onError: e => Console.WriteLine("ERROR: " + e),
        onCompleted: () => Console.WriteLine("DONE")
    );
```

The outstanding piece for this version is to support multiple subscriber types.
