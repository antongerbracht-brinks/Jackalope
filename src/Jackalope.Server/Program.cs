using System.Runtime.InteropServices;
using Jackalope;

// Jackalope standalone broker.
//
// stdout is reserved for machine-readable output: exactly one "PORT=<n>" line followed by one "URI=<amqp uri>"
// line, emitted once the listener is bound. Everything else (logs, errors) goes to stderr.

const string Usage = """
    Usage: jackalope [options]

      --host <address>        Address to bind (default 127.0.0.1)
      --port <n>              Port to bind; 0 = OS-assigned ephemeral port (default 0)
      --username <name>       Require this PLAIN username (default: accept any)
      --password <secret>     Require this PLAIN password (default: accept any)
      --heartbeat <seconds>   Heartbeat interval proposed to clients (default 60; 0 disables)
      --frame-max <bytes>     Frame size proposed to clients (default 131072)
      --exit-on-stdin-close   Shut down when stdin reaches EOF (lets a parent process
                              guarantee cleanup even if it dies without stopping us)
      --verbose               Log connection lifecycle and protocol errors to stderr
      --help                  Show this help

    On startup prints to stdout:
      PORT=<bound port>
      URI=amqp://<user>:<pass>@<host>:<port>/
    """;

JackalopeBrokerOptions? options;
bool exitOnStdinClose = false, verbose = false;
try
{
    (options, exitOnStdinClose, verbose) = ParseArguments(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    Console.Error.WriteLine(Usage);
    return 2;
}

if (options is null)
{
    Console.Out.WriteLine(Usage);
    return 0;
}

if (verbose)
{
    options = options with { Logger = message => Console.Error.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {message}") };
}

await using var broker = new JackalopeBroker(options);
try
{
    await broker.StartAsync();
}
catch (Exception ex)
{
    Console.Error.WriteLine($"error: failed to start: {ex.Message}");
    return 1;
}

Console.Out.WriteLine($"PORT={broker.Port}");
Console.Out.WriteLine($"URI={broker.GetConnectionString()}");
Console.Out.Flush();
Console.Error.WriteLine($"Jackalope listening on {broker.Hostname}:{broker.Port} (Ctrl+C to stop)");

var stop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
using var sigint = PosixSignalRegistration.Create(PosixSignal.SIGINT, ctx => { ctx.Cancel = true; stop.TrySetResult(); });
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; stop.TrySetResult(); });

if (exitOnStdinClose)
{
    _ = Task.Run(async () =>
    {
        try
        {
            using var stdin = Console.OpenStandardInput();
            var buffer = new byte[256];
            while (await stdin.ReadAsync(buffer) > 0)
            {
            }
        }
        catch
        {
            // Treat a broken stdin like EOF.
        }

        stop.TrySetResult();
    });
}

await stop.Task;
Console.Error.WriteLine("Jackalope shutting down");
await broker.StopAsync();
return 0;

static (JackalopeBrokerOptions? Options, bool ExitOnStdinClose, bool Verbose) ParseArguments(string[] args)
{
    var options = new JackalopeBrokerOptions();
    bool exitOnStdinClose = false, verbose = false;

    for (int i = 0; i < args.Length; i++)
    {
        string arg = args[i];
        string Value() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{arg} requires a value");
        int Number(int min, int max)
        {
            string raw = Value();
            return int.TryParse(raw, out int n) && n >= min && n <= max
                ? n
                : throw new ArgumentException($"{arg} must be an integer between {min} and {max} (got '{raw}')");
        }

        switch (arg)
        {
            case "--host": options = options with { Host = Value() }; break;
            case "--port": options = options with { Port = Number(0, 65535) }; break;
            case "--username": options = options with { Username = Value() }; break;
            case "--password": options = options with { Password = Value() }; break;
            case "--heartbeat": options = options with { HeartbeatSeconds = (ushort)Number(0, ushort.MaxValue) }; break;
            case "--frame-max": options = options with { FrameMax = Number(4096, int.MaxValue) }; break;
            case "--exit-on-stdin-close": exitOnStdinClose = true; break;
            case "--verbose": verbose = true; break;
            case "--help" or "-h" or "-?": return (null, false, false);
            default: throw new ArgumentException($"unknown option '{arg}'");
        }
    }

    return (options, exitOnStdinClose, verbose);
}
