using System.Text.Json;
using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using Org.Apache.Rocketmq;

namespace MessageBus.Service.Services.MessageTargets;

/// <summary>
/// RocketMQ消息目标实现
/// </summary>
public class RocketMQMessageTarget : IMessageTarget
{
    private readonly RocketMQMessageTargetConfig _config;
    private readonly Producer.Builder? _producerBuilder;
    private Producer? _producer;
    public bool IsMessageBusTarget { get; }
    public static readonly string AccessKey = "TestAccessKey";
    public static readonly string AccessSecret = "TestAccessSecret";

    public RocketMQMessageTarget(RocketMQMessageTargetConfig config)
    {
        _config = config;
        IsMessageBusTarget = config.IsMessageBusTarget;
        var credentialsProvider = new StaticSessionCredentialsProvider(AccessKey, AccessSecret);
        var clientConfig = new ClientConfig.Builder()
            .SetEndpoints(config.ConnectionString)
            .SetCredentialsProvider(credentialsProvider)
            .Build();
        _producerBuilder = new Producer.Builder()
            .SetTopics(config.Topic)
            .SetClientConfig(clientConfig);
    }

    public async Task<bool> SendMessageAsync(MessageModel message)
    {
        if (message is null)
        {
            return true;
        }
        if (_producer is null && _producerBuilder is not null)
        {
            _producer = await _producerBuilder.Build();
        }
        try
        {
            var messageBody = JsonSerializer.SerializeToUtf8Bytes(message!.Content);
            var rocketMessage = new Message.Builder()
                .SetTopic(_config.Topic)
                .SetBody(messageBody)
                .SetTag(_config.Tag)
                .SetKeys("yourMessageKey-7044358f98fc")
                .Build();
            var sendResult = await _producer!.Send(rocketMessage);
            Console.WriteLine($"RocketMQ消息发送成功: 消息ID={sendResult.MessageId}, 主题={_config.Topic}");
            return string.IsNullOrWhiteSpace(sendResult.MessageId);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RocketMQ消息发送失败: {ex.Message}, 消息：{JsonSerializer.Serialize(message)}");
            return false;
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
        _producer = null;
    }

    public async Task DisposeAsync()
    {
        if (_producer is not null)
        {
            await _producer!.DisposeAsync();
            _producer = null;
        }
    }
}