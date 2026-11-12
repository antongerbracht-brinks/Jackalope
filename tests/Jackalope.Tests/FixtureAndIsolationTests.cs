using System.Diagnostics;
using RabbitMQ.Client;
using static Jackalope.Tests.TestHelpers;

namespace Jackalope.Tests;

/// <summary>Stage 6: test-fixture API, per-instance isolation, and the standalone child-process mode.</summary>
public class FixtureAndIsolationTests
{
    [Fact]
    public async Task Builder_produces_a_working_broker_with_configured_credentials()
    {
        await using var rabbit = new JackalopeBuilder().WithUsername("app").WithPassword("p@ss:/word").Build();
        await rabbit.StartAsync();

        Assert.NotEqual(0, rabbit.Port);
        Assert.Equal($"amqp://app:p%40ss%3A%2Fword@127.0.0.1:{rabbit.Port}/", rabbit.GetConnectionString());

        await using var connection = await new ConnectionFactory { Uri = new Uri(rabbit.GetConnectionString()) }.CreateConnectionAsync();
        Assert.True(connection.IsOpen);
    }

    [Fact]
    public async Task Concurrent_instances_get_distinct_ports_and_share_no_state()
    {
        const int instances = 12;
        var brokers = await Task.WhenAll(Enumerable.Range(0, instances).Select(_ => StartBrokerAsync()));
        try
        {
            Assert.Equal(instances, brokers.Select(b => b.Port).Distinct().Count());

            // Same queue name everywhere, a different message in each broker.
            await Task.WhenAll(brokers.Select(async (broker, i) =>
            {
                await using var connection = await ConnectAsync(broker);
                await using var channel = await connection.CreateChannelAsync();
                await channel.QueueDeclareAsync("shared-name", false, false, false);
                await channel.PublishAsync("", "shared-name", $"from-{i}");
            }));

            await Task.WhenAll(brokers.Select(async (broker, i) =>
            {
                Assert.Equal(1, broker.GetQueue("shared-name")!.MessageCount);
                await using var connection = await ConnectAsync(broker);
                await using var channel = await connection.CreateChannelAsync();
                var consumer = await TestConsumer.StartAsync(channel, "shared-name", autoAck: true);
                Assert.Equal($"from-{i}", (await consumer.NextAsync()).Text);
                await consumer.AssertNothingAsync(TimeSpan.FromMilliseconds(100));
            }));
        }
        finally
        {
            await Task.WhenAll(brokers.Select(b => b.DisposeAsync().AsTask()));
        }
    }

    [Fact]
    public async Task Fixed_port_can_be_requested_and_a_broker_cannot_be_started_twice()
    {
        int freePort;
        await using (var probe = await StartBrokerAsync())
        {
            freePort = probe.Port;
        }

        await using var broker = new JackalopeBroker(new JackalopeBrokerOptions { Port = freePort });
        await broker.StartAsync();
        Assert.Equal(freePort, broker.Port);
        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync());
    }

    [Fact]
    public async Task Child_process_broker_reports_its_port_and_serves_real_clients()
    {
        var messages = new List<string>();
        await using var rabbit = new JackalopeBuilder()
            .WithUsername("ci")
            .WithPassword("ci")
            .WithLogger(m => { lock (messages) { messages.Add(m); } })
            .BuildProcess();
        await rabbit.StartAsync();

        Assert.NotNull(rabbit.ProcessId);
        Assert.NotEqual(Environment.ProcessId, rabbit.ProcessId);

        await using (var connection = await ConnectAsync(rabbit))
        await using (var channel = await connection.CreateChannelAsync())
        {
            await channel.ExchangeDeclareAsync("t", ExchangeType.Topic);
            await channel.QueueDeclareAsync("q", false, false, false);
            await channel.QueueBindAsync("q", "t", "a.#");
            await channel.PublishAsync("t", "a.b", "across processes");
            var consumer = await TestConsumer.StartAsync(channel, "q");
            var message = await consumer.NextAsync();
            Assert.Equal("across processes", message.Text);
            await channel.BasicAckAsync(message.DeliveryTag, false);
        }

        int pid = rabbit.ProcessId!.Value;
        await rabbit.StopAsync();
        Assert.True(Process.GetProcesses().All(p => p.Id != pid) || Process.GetProcessById(pid).HasExited);
        lock (messages)
        {
            Assert.Contains(messages, m => m.Contains("listening on"));
        }
    }

    [Fact]
    public async Task Standalone_server_prints_port_and_uri_on_stdout_and_exits_when_stdin_closes()
    {
        var (fileName, prefix) = JackalopeProcess.ResolveServer(null);
        var startInfo = new ProcessStartInfo(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardInput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (string arg in prefix.Concat(["--port", "0", "--exit-on-stdin-close"]))
        {
            startInfo.ArgumentList.Add(arg);
        }

        using var process = Process.Start(startInfo)!;
        try
        {
            string? portLine = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(30));
            string? uriLine = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Matches(@"^PORT=\d+$", portLine);
            int port = int.Parse(portLine![5..]);
            Assert.Equal($"URI=amqp://guest:guest@127.0.0.1:{port}/", uriLine);

            var factory = new ConnectionFactory { Uri = new Uri(uriLine![4..]), AutomaticRecoveryEnabled = false };
            await using (var connection = await factory.CreateConnectionAsync())
            {
                Assert.True(connection.IsOpen);
            }

            process.StandardInput.Close();
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(0, process.ExitCode);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(true);
            }
        }
    }

    [Fact]
    public async Task Missing_server_executable_gives_an_actionable_error()
    {
        await using var rabbit = new JackalopeProcess(serverPath: Path.Combine(Path.GetTempPath(), "definitely-not-here.exe"));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => rabbit.StartAsync());
        Assert.Contains("JACKALOPE_SERVER_PATH", ex.Message);
    }
}

/// <summary>
/// Shows the intended xUnit usage: one broker per test class via IAsyncLifetime (use a collection fixture to share
/// one across classes). The same shape works for MSTest with [ClassInitialize]/[ClassCleanup].
/// </summary>
public sealed class ExampleFixtureUsage : IAsyncLifetime
{
    private readonly JackalopeBroker _rabbit = new JackalopeBuilder().Build();

    public Task InitializeAsync() => _rabbit.StartAsync();

    public Task DisposeAsync() => _rabbit.DisposeAsync().AsTask();

    [Fact]
    public async Task Service_under_test_can_use_the_broker_like_real_rabbitmq()
    {
        var factory = new ConnectionFactory { Uri = new Uri(_rabbit.GetConnectionString()) };
        await using var connection = await factory.CreateConnectionAsync("service-under-test");
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeclareAsync("inbox", durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" });
        await channel.BasicPublishAsync("", "inbox", Utf8("ping"));

        await EventuallyAsync(() => _rabbit.GetQueue("inbox")?.MessageCount == 1, "message stored");
    }
}
