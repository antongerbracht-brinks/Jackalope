# Jackalope

**A lightweight mock RabbitMQ broker for .NET functional tests.** Jackalope speaks enough real
AMQP 0-9-1 on the wire that the official [`RabbitMQ.Client`](https://www.nuget.org/packages/RabbitMQ.Client)
(v7.x, async API) connects to it with an ordinary `ConnectionFactory().CreateConnectionAsync()` and can't tell
the difference for the supported feature set: declare exchanges and queues, bind them, publish, consume, ack/nack.

It starts in milliseconds, keeps everything in memory, and each instance is completely independent, so every
test run (or every test) gets its own private broker on its own ephemeral port.

> Like the animal it's named after, it's not a real rabbit. It looks enough like one for your tests.

---

## Why this exists

We need to functional-test .NET services that use `RabbitMQ.Client` **directly** (not through an injectable
abstraction we could fake). The usual options don't work in our CI environment:

| Option | Problem |
| --- | --- |
| Testcontainers / Docker RabbitMQ | Docker is not available on our build agents. |
| One shared, natively-installed RabbitMQ | Many pipeline builds run in parallel on the same agents. A shared broker means queues, exchanges and messages cross-talk between concurrent test runs, which causes flaky, order-dependent failures. |
| Mocking `IConnection`/`IChannel` | Doesn't exercise the real client, its threading, framing, or error behaviour, and is a lot of brittle setup. |

Jackalope's answer is a broker cheap enough to run **one fresh, fully isolated instance per test run, per
pipeline job**, on an OS-assigned port, with **zero shared state**: no files, no named pipes, no statics, no
singletons. Ten jobs on one agent run ten brokers that know nothing about each other.

---

## Repository layout

```
src/Jackalope/           Library + NuGet package "Jackalope": the broker engine and the test-fixture API
src/Jackalope.Server/    Console app / dotnet tool "jackalope" for standalone and CI-step use
tests/Jackalope.Tests/   xUnit suite that validates everything against the real RabbitMQ.Client 7.x
```

Requirements: .NET 10 SDK (pinned in `global.json`).

```bash
dotnet build
dotnet test
```

---

## Using it from a test project (recommended: in-process)

Reference the `Jackalope` package (or project). The API is modelled on Testcontainers' `RabbitMqContainer`:
`StartAsync()`, `DisposeAsync()`, `Hostname`, `Port`, `GetConnectionString()`.

```csharp
using Jackalope;
using RabbitMQ.Client;

await using var rabbit = new JackalopeBuilder().Build(); // port 0 = OS-assigned
await rabbit.StartAsync();

var factory = new ConnectionFactory { Uri = new Uri(rabbit.GetConnectionString()) };
// e.g. amqp://guest:guest@127.0.0.1:53817/
await using var connection = await factory.CreateConnectionAsync();
```

Hand `rabbit.GetConnectionString()` (or `rabbit.Hostname` and `rabbit.Port`) to the service under test the same way
you'd hand it a real broker's address.

### xUnit

```csharp
public sealed class OrderServiceTests : IAsyncLifetime
{
    private readonly JackalopeBroker _rabbit = new JackalopeBuilder().Build();

    public Task InitializeAsync() => _rabbit.StartAsync();
    public Task DisposeAsync() => _rabbit.DisposeAsync().AsTask();

    [Fact]
    public async Task Publishes_order_created()
    {
        var service = new OrderService(new Uri(_rabbit.GetConnectionString()));
        await service.CreateOrderAsync(42);

        Assert.Equal(1, _rabbit.GetQueue("orders.created")!.MessageCount);
    }
}
```

To share one broker across several test classes, use an xUnit collection fixture. Separate test classes run in
parallel by default, so giving each class its own broker is usually simplest. Startup is fast enough for that.

### MSTest

```csharp
[TestClass]
public class OrderServiceTests
{
    private static JackalopeBroker _rabbit = null!;

    [ClassInitialize]
    public static async Task Init(TestContext _) { _rabbit = new JackalopeBuilder().Build(); await _rabbit.StartAsync(); }

    [ClassCleanup]
    public static async Task Cleanup() => await _rabbit.DisposeAsync();
}
```

### Builder options

| Method | Default | Notes |
| --- | --- | --- |
| `WithPort(int)` | `0` | 0 asks the OS for a free port. Use this for parallel CI. |
| `WithHost(string)` | `127.0.0.1` | Loopback avoids firewall prompts. |
| `WithUsername(string)` / `WithPassword(string)` | accept anything | When set, wrong credentials fail like RabbitMQ does (`AuthenticationFailureException`). |
| `WithHeartbeat(TimeSpan)` | 60 s | Proposed in `connection.tune`. The client may negotiate it down, or to 0 to disable. |
| `WithFrameMax(int)` | 131072 | Proposed in `connection.tune`. |
| `WithLogger(Action<string>)` | none | Connection lifecycle and protocol-error diagnostics. |

`JackalopeBrokerOptions` exposes the same settings, plus `ChannelMax` and `MaxMessageSize`, if you prefer
`new JackalopeBroker(new JackalopeBrokerOptions { ... })`.

### Assertion helpers (introspection)

These aren't part of AMQP. They give tests a direct view of broker state:

```csharp
rabbit.GetQueues();          // name, ready message count, consumer count, flags, arguments
rabbit.GetQueue("orders");   // or null
rabbit.GetExchanges();       // including bindings
rabbit.ConnectionCount;
```

`MessageCount` counts messages that are *ready*. Delivered-but-unacked messages aren't included, which matches
RabbitMQ's `queue.declare-ok` message count.

### Child-process mode

`JackalopeBuilder().BuildProcess()` returns a `JackalopeProcess` with the same API. It runs the broker as a
separate OS process and reads the port from that process's stdout. The child is started with
`--exit-on-stdin-close`, so if the test process crashes, the broker exits too and isn't left orphaned on the agent.

The server executable is located in this order:

1. the path passed to `BuildProcess(path)` / `new JackalopeProcess(options, path)`
2. the `JACKALOPE_SERVER_PATH` environment variable
3. `Jackalope.Server.dll` next to the running test assembly (present if the test project references
   `Jackalope.Server`)
4. the `jackalope` command on `PATH` (`dotnet tool install -g Jackalope.Server`)

A path ending in `.dll` is launched with `dotnet`. For .NET test projects the in-process broker is simpler and
faster, and it gives the same isolation. Use process mode when you specifically want the broker out-of-process.

---

## Running it standalone (CI steps, other languages, manual testing)

```bash
dotnet run --project src/Jackalope.Server -- --port 0
# or, once packed/installed as a tool:
dotnet tool install -g Jackalope.Server
jackalope --port 0
```

Once the listener is bound, stdout carries exactly two machine-readable lines. All logs go to stderr.

```
PORT=53817
URI=amqp://guest:guest@127.0.0.1:53817/
```

| Option | Default | Description |
| --- | --- | --- |
| `--host <address>` | `127.0.0.1` | Address to bind. |
| `--port <n>` | `0` | Port to bind; `0` = OS-assigned ephemeral port. |
| `--username <name>` / `--password <secret>` | accept any | Credentials to enforce (PLAIN). |
| `--heartbeat <seconds>` | `60` | Heartbeat interval to propose; `0` disables. |
| `--frame-max <bytes>` | `131072` | Frame size to propose. |
| `--exit-on-stdin-close` | off | Exit when stdin reaches EOF, so a parent process can guarantee cleanup. |
| `--verbose` | off | Log connections and protocol errors to stderr. |

It stops cleanly on Ctrl+C or SIGTERM (and on stdin EOF when that flag is set), closing client connections with
`320 CONNECTION_FORCED` as a real broker shutdown would.

Example CI step (bash):

```bash
coproc JACK { jackalope --port 0 --exit-on-stdin-close; }
read -r line <&"${JACK[0]}"; export RABBITMQ_PORT="${line#PORT=}"
dotnet test   # tests read RABBITMQ_PORT
```

---

## What's implemented (AMQP 0-9-1 subset)

The test suite checks every item below against the real `RabbitMQ.Client` 7.2.2. Each feature's acceptance bar is
that the real client behaves as it would against RabbitMQ.

| Area | Support |
| --- | --- |
| Protocol header | `AMQP 0-0-9-1` negotiated; any other header gets the supported header back and the socket is closed, per spec. |
| Connection | `start/start-ok` (PLAIN), `tune/tune-ok`, `open/open-ok`, `close/close-ok` in both directions. Auth failures use `authentication_failure_close`, as RabbitMQ does. |
| Channels | `open/open-ok`, `close/close-ok` in both directions, full multiplexing over one TCP connection, per-channel delivery tags. A channel-level error closes only that channel. |
| Exchanges | `exchange.declare` (incl. passive) for **direct**, **topic** (real `*`/`#` matching) and **fanout**. Built-ins: default `""`, `amq.direct`, `amq.topic`, `amq.fanout`. Redeclaring with different properties gives `406 PRECONDITION_FAILED`. |
| Queues | `queue.declare` (incl. passive, server-named `amq.gen-*`, exclusive, auto-delete), `queue.bind`, `queue.unbind`. **Unrecognised arguments (`x-queue-type: quorum`, `x-message-ttl`, ...) are accepted and ignored.** |
| Basic | `qos` (per-consumer and `global` channel-wide prefetch), `consume` (incl. exclusive, auto-ack), `cancel`, `publish`, `deliver`, `ack` (incl. `multiple`), `nack` (incl. `multiple`, `requeue`), `reject`. Message properties and header tables pass through byte-for-byte. |
| Framing | Type/channel/size/payload/`0xCE` framing; content split into body frames by the negotiated frame-max and reassembled on the way in. Oversized or malformed frames get `501 FRAME_ERROR`. |
| Field tables | Full encode/decode of RabbitMQ's field-table dialect (all value types incl. nested tables and arrays). |
| Heartbeats | Negotiated in tune. The broker sends a heartbeat every half interval and drops a client that's silent for two intervals. |
| Requeue semantics | Unacked messages are requeued (marked `redelivered`) when a channel or connection closes or on `nack/reject requeue=true`. They go back to the head of the queue, in their original order. |
| Small extras | Publisher confirms (`confirm.select`, every publish is acked immediately) and `basic.return` for `mandatory` unroutable messages. These are included because 7.x clients with confirmation tracking would otherwise hang or misreport. |

Anything outside this list gets a loud failure instead of silent misbehaviour: the connection is closed with
`540 NOT_IMPLEMENTED` and a reply text naming the method (e.g. `NOT_IMPLEMENTED - queue.delete is not supported by Jackalope`).

---

## Non-goals and known limitations (v1)

These are deliberate omissions, not bugs:

- **No persistence or durability.** Everything is in memory. `durable` and `delivery-mode=2` are accepted and
  remembered for equivalence checks, but nothing survives the instance.
- **No clustering, HA, mirroring or quorum semantics.** `x-queue-type` and friends are accepted and ignored.
- **No management HTTP API.** Use the in-process introspection methods instead.
- **No AMQP 1.0, no TLS (`amqps://`), and only the PLAIN SASL mechanism.**
- **Publisher confirms only as far as trivially needed.** Every publish is confirmed immediately; there's no nack-on-failure.
- **No method coverage beyond the list above.** In particular these aren't implemented and close the connection
  with `540`: `queue.delete`, `queue.purge`, `exchange.delete`, `exchange.bind/unbind` (exchange-to-exchange),
  `basic.get`, `basic.recover`, `channel.flow`, `tx.*`, `connection.update-secret`.
  (You rarely need deletes in tests: each test run gets a fresh broker.)
- **Headers exchanges aren't supported.** Declaring one fails with `540`.
- **Queue/consumer arguments aren't honoured:** no TTLs, no max-length, no dead-lettering (`nack`/`reject`
  with `requeue=false` simply discards), no priorities, no single-active-consumer, no consumer priorities.
- **Virtual hosts aren't isolated.** Any vhost name is accepted and all share one namespace per broker instance.
  Isolation comes from running separate instances.
- **`immediate=true` publishes** are rejected with `540`, as RabbitMQ does.
- **Built for correctness, not throughput.** Each broker instance uses one coarse lock. That's plenty for
  functional tests and not intended for load testing.

---

## How it's validated

`tests/Jackalope.Tests` uses the real `RabbitMQ.Client` 7.x throughout. A tiny raw-socket client is used only to
provoke wire-level situations a well-behaved real client can't produce, such as going silent to trigger a heartbeat
timeout. The suite covers:

- connect/disconnect, credentials, server properties, broker shutdown (`320`)
- every exchange type, wildcard routing (unit-level matcher table + end-to-end), unbind, mandatory returns
- queue declare with arbitrary arguments, server-named, exclusive, and auto-delete queues
- publish → `AsyncEventingBasicConsumer` → ack/nack/reject round trips, asserting body **and every message property**
- bodies from 0 bytes to 5 MB across frame boundaries, including a forced 4 KB frame-max
- prefetch (per-consumer and global), requeue ordering, redelivered flags
- 16 concurrent channels on one connection, 8 connections × 4 channels, channel-error isolation, cross-connection requeue
- heartbeats: real client with a 1 s heartbeat kept alive by broker heartbeats; dead-client detection
- 12 concurrent broker instances with identical queue names and no cross-talk
- the standalone executable's `PORT=` contract and stdin-EOF shutdown; child-process fixture

---

## Versioning and packaging

```bash
dotnet pack -c Release -o artifacts
# artifacts/Jackalope.<ver>.nupkg         library + fixture API
# artifacts/Jackalope.Server.<ver>.nupkg  dotnet tool "jackalope"
```
