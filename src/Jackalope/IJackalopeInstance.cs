using System.Net;
using System.Net.Sockets;

namespace Jackalope;

/// <summary>
/// A running (or startable) Jackalope broker, either in-process (<see cref="JackalopeBroker"/>) or a child
/// process (<see cref="JackalopeProcess"/>). Shaped like Testcontainers' RabbitMqContainer.
/// </summary>
public interface IJackalopeInstance : IAsyncDisposable
{
    /// <summary>Host name clients should connect to.</summary>
    string Hostname { get; }

    /// <summary>The actual bound TCP port (resolved after <see cref="StartAsync"/> when port 0 was requested).</summary>
    int Port { get; }

    string Username { get; }

    string Password { get; }

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>An <c>amqp://user:pass@host:port/</c> URI suitable for <c>ConnectionFactory.Uri</c>.</summary>
    string GetConnectionString();
}

internal static class ConnectionStrings
{
    public static string Build(string host, int port, string username, string password)
    {
        string hostPart = IPAddress.TryParse(host, out var ip) && ip.AddressFamily == AddressFamily.InterNetworkV6
            ? $"[{host}]"
            : host;
        return $"amqp://{Uri.EscapeDataString(username)}:{Uri.EscapeDataString(password)}@{hostPart}:{port}/";
    }

    /// <summary>Maps wildcard bind addresses to the matching loopback address so the URI is connectable.</summary>
    public static string ConnectableHost(IPAddress bound, string configuredHost)
    {
        if (bound.Equals(IPAddress.Any))
        {
            return IPAddress.Loopback.ToString();
        }

        if (bound.Equals(IPAddress.IPv6Any))
        {
            return IPAddress.IPv6Loopback.ToString();
        }

        return IPAddress.TryParse(configuredHost, out _) ? bound.ToString() : configuredHost;
    }
}
