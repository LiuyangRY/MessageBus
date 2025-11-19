using System.Text.Json;
using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;
using MessageBus.Service.Services.MessageTargets;
using Org.Apache.Rocketmq;

namespace MessageBus.Service.Services.MessageSources;

/// <summary>
/// RocketMQ消息源实现
/// </summary>
public class RocketMQMessageSource : IMessageSource
{
    private SimpleConsumer.Builder? _consumerBuilder;
    private SimpleConsumer? _consumer;
    private bool _isConsuming;

    public EnumChannelType SourceChannelType => EnumChannelType.RocketMQ;

    public event Func<MessageModel, Task<bool>>? OnMessageReceived;
    private readonly RocketMQMessageSourceConfig _config;

    public RocketMQMessageSource(RocketMQMessageSourceConfig config)
    {
        _config = config;
        var credentialsProvider = new StaticSessionCredentialsProvider(RocketMQMessageTarget.AccessKey, RocketMQMessageTarget.AccessSecret);
        var clientConfig = new ClientConfig.Builder()
            .SetEndpoints(config.ConnectionString)
            .SetCredentialsProvider(credentialsProvider)
            .Build();
        var subscription = new Dictionary<string, FilterExpression>
        {
            { config.TopicName, new FilterExpression("*") }
        };
        _consumerBuilder = new SimpleConsumer.Builder()
            .SetClientConfig(clientConfig)
            .SetConsumerGroup(config.ConsumerGroup)
            .SetAwaitDuration(TimeSpan.FromSeconds(15))
            .SetSubscriptionExpression(subscription);
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken = default)
    {
        if (_isConsuming)
            return;
        if (_consumer is null && _consumerBuilder is not null)
        {
            _consumer = await _consumerBuilder.Build();
        }
        try
        {
            _isConsuming = true;
            _ = Task.Run(async () => await ConsumeMessagesAsync(cancellationToken), cancellationToken);
            Console.WriteLine($"RocketMQ消费者已启动: {_config.Name}, 主题: {_config.TopicName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RocketMQ消费者启动失败: {ex.Message}");
            throw;
        }
    }

    private async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        while (_isConsuming && !cancellationToken.IsCancellationRequested)
        {

            try
            {
                var messageViews = await _consumer!.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    var messageModel = DeserializeMessage(message);
                    if (messageModel != null && OnMessageReceived != null)
                    {
                        var success = await OnMessageReceived(messageModel);
                        if (success)
                        {
                            await _consumer!.Ack(message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"RocketMQ消费循环异常: {ex.Message}");
                await Task.Delay(1000, cancellationToken);
            }
        }
    }

    public async Task StopConsumingAsync()
    {
        if (!_isConsuming || _consumer is null)
            return;

        try
        {
            await _consumer.DisposeAsync();
            _isConsuming = false;
            Console.WriteLine($"RocketMQ消费者已停止: {_config.Name}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RocketMQ消费者停止异常: {ex.Message}");
        }
    }

    private MessageModel? DeserializeMessage(MessageView message)
    {
        try
        {
            var body = System.Text.Encoding.UTF8.GetString(message.Body);
            var messageModel = JsonSerializer.Deserialize<MessageModel>(body);
            return messageModel;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"RocketMQ消息反序列化失败: {ex.Message}");
            return null;
        }
    }

    public async Task DisposeAsync()
    {
        if (_consumer != null)
        {
            await _consumer.DisposeAsync();
            _consumer = null;
        }
    }

    public void Dispose()
    {
        _consumer?.Dispose();
        _consumer = null;
    }
}