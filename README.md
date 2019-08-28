# Rx for Event Store

This is an experimental fork of [EventStore](https://github.com/EventStore/EventStore) to support Reactive Extensions (Rx).

[Qube](https://github.com/JasonKStevens/Qube) allows linq queries to be written on the client, executed inside of EventStore and the results streamed back via [gRpc](https://grpc.io/).

```c#
var options = new StreamDbContextOptionsBuilder()
    .UseEventStore("127.0.0.1:5001")
    .Options;

new EventStoreContext(options)
    .FromAll()
    .Where(e => e.EventType == "CustomerCreatedEvent")
    .Where(e => new DateTime(2018, 3, 1) <= e.Created)
    .TakeWhile(e => e.Created < new DateTime(2018, 4, 1))
    .Select(e => e.Data)
    .Subscribe(
        onNext: s =>
        {
            var @event = JsonConvert.DeserializeObject<CustomerCreatedEvent>(s);
            Console.WriteLine($"{@event.CustomerId}: {@event.Email}");
        },
        onError: e => Console.WriteLine("ERROR: " + e),
        onCompleted: () => Console.WriteLine("DONE")
    );
```

## Getting Started

1. Clone, build and run this fork of EventStore
2. Set up some test events
3. Clone Qube
4. Edit Qube.EventStore.Client.Program and write your Rx query
5. Build and run Qube.EventStore.Client

## The Plan

See the [Qube](https://github.com/JasonKStevens/Qube) project for a rough list of intended features.
