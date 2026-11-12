using System.Diagnostics;
using System.Globalization;

namespace Jackalope;

/// <summary>
/// Runs a Jackalope broker as a child process (the <c>Jackalope.Server</c> executable) and discovers its port from
/// the <c>PORT=</c> line it prints. The child is started with <c>--exit-on-stdin-close</c>, so it shuts itself down
/// if this process dies without disposing it — no orphaned brokers on CI agents.
/// </summary>
/// <remarks>
/// The server is located in this order: the explicit path passed to the constructor; the
/// <c>JACKALOPE_SERVER_PATH</c> environment variable; <c>Jackalope.Server.dll</c> next to the running application
/// (present when the test project references the Jackalope.Server project/package); finally the <c>jackalope</c>
/// command on PATH (installed via <c>dotnet tool install -g Jackalope.Server</c>).
/// A path ending in <c>.dll</c> is launched with <c>dotnet</c>.
/// </remarks>
public sealed class JackalopeProcess : IJackalopeInstance
{
    public const string ServerPathEnvironmentVariable = "JACKALOPE_SERVER_PATH";

    private readonly JackalopeBrokerOptions _options;
    private readonly string? _serverPath;
    private Process? _process;

    public JackalopeProcess(JackalopeBrokerOptions? options = null, string? serverPath = null)
    {
        _options = options ?? new JackalopeBrokerOptions();
        _options.Validate();
        _serverPath = serverPath;
    }

    /// <summary>How long to wait for the child to report its port.</summary>
    public TimeSpan StartupTimeout { get; init; } = TimeSpan.FromSeconds(30);

    public string Hostname { get; private set; } = "127.0.0.1";

    public int Port { get; private set; }

    public string Username => _options.Username ?? "guest";

    public string Password => _options.Password ?? "guest";

    /// <summary>OS process id of the running child, or null.</summary>
    public int? ProcessId => _process is { HasExited: false } p ? p.Id : null;

    public string GetConnectionString() => ConnectionStrings.Build(Hostname, Port, Username, Password);

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_process is not null)
        {
            throw new InvalidOperationException("The process has already been started. Create a new instance instead.");
        }

        var (fileName, prefixArguments) = ResolveServer(_serverPath);
        var startInfo = new ProcessStartInfo(fileName)
        {
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        foreach (string argument in prefixArguments.Concat(BuildArguments()))
        {
            startInfo.ArgumentList.Add(argument);
        }

        var portReported = new TaskCompletionSource<(int Port, string? Uri)>(TaskCreationOptions.RunContinuationsAsynchronously);
        int? port = null;
        var stderr = new List<string>();

        var process = new Process { StartInfo = startInfo, EnableRaisingEvents = true };
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is null)
            {
                portReported.TrySetException(new InvalidOperationException(
                    $"Jackalope server exited before reporting its port. stderr:{Environment.NewLine}{string.Join(Environment.NewLine, stderr)}"));
            }
            else if (e.Data.StartsWith("PORT=", StringComparison.Ordinal) &&
                     int.TryParse(e.Data.AsSpan(5), NumberStyles.None, CultureInfo.InvariantCulture, out int value))
            {
                port = value;
            }
            else if (e.Data.StartsWith("URI=", StringComparison.Ordinal) && port is int reported)
            {
                portReported.TrySetResult((reported, e.Data[4..]));
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is null)
            {
                return;
            }

            lock (stderr)
            {
                if (stderr.Count < 200)
                {
                    stderr.Add(e.Data);
                }
            }

            _options.Log($"[jackalope-server] {e.Data}");
        };

        try
        {
            process.Start();
        }
        catch (Exception ex)
        {
            process.Dispose();
            throw new InvalidOperationException(
                $"Could not launch the Jackalope server '{fileName}'. Pass its path explicitly, set {ServerPathEnvironmentVariable}, " +
                "reference the Jackalope.Server package, or install the 'jackalope' dotnet tool.", ex);
        }

        _process = process;
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        try
        {
            (Port, _) = await portReported.Task.WaitAsync(StartupTimeout, cancellationToken).ConfigureAwait(false);
            Hostname = ConnectionStrings.ConnectableHost(
                System.Net.IPAddress.TryParse(_options.Host, out var ip) ? ip : System.Net.IPAddress.Loopback, _options.Host);
        }
        catch
        {
            await KillAsync().ConfigureAwait(false);
            throw;
        }
    }

    private IEnumerable<string> BuildArguments()
    {
        yield return "--host";
        yield return _options.Host;
        yield return "--port";
        yield return _options.Port.ToString(CultureInfo.InvariantCulture);
        yield return "--heartbeat";
        yield return _options.HeartbeatSeconds.ToString(CultureInfo.InvariantCulture);
        yield return "--frame-max";
        yield return _options.FrameMax.ToString(CultureInfo.InvariantCulture);
        if (_options.Username is not null)
        {
            yield return "--username";
            yield return _options.Username;
        }

        if (_options.Password is not null)
        {
            yield return "--password";
            yield return _options.Password;
        }

        if (_options.Logger is not null)
        {
            yield return "--verbose";
        }

        yield return "--exit-on-stdin-close";
    }

    internal static (string FileName, string[] PrefixArguments) ResolveServer(string? explicitPath)
    {
        string? path = explicitPath ?? Environment.GetEnvironmentVariable(ServerPathEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(path))
        {
            string candidate = Path.Combine(AppContext.BaseDirectory, "Jackalope.Server.dll");
            path = File.Exists(candidate) ? candidate : null;
        }

        if (path is null)
        {
            return ("jackalope", []);
        }

        return path.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
            ? (DotnetHost(), [path])
            : (path, []);
    }

    private static string DotnetHost()
    {
        string? host = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        if (!string.IsNullOrEmpty(host) && File.Exists(host))
        {
            return host;
        }

        string? current = Environment.ProcessPath;
        if (current is not null &&
            Path.GetFileNameWithoutExtension(current).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
        {
            return current;
        }

        return "dotnet";
    }

    /// <summary>Asks the child to shut down gracefully (by closing its stdin), killing it if it does not exit promptly.</summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        var process = _process;
        if (process is null || process.HasExited)
        {
            return;
        }

        try
        {
            process.StandardInput.Close();
            await process.WaitForExitAsync(cancellationToken).WaitAsync(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException)
        {
            await KillAsync().ConfigureAwait(false);
        }
    }

    private async Task KillAsync()
    {
        var process = _process;
        if (process is null)
        {
            return;
        }

        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
        }
        catch
        {
            // Best effort.
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _process?.Dispose();
    }
}
