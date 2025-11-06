using System.Text.Json;
using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageBus.Service.Services.MessageSources;

public class RabbitMQMessageSource : IMessageSource
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _queueName;
    private bool _isConsuming = false;

    public EnumChannelType SourceChannelType => EnumChannelType.RabbitMQ;

    public event Func<MessageModel, Task<bool>>? OnMessageReceived;

    public RabbitMQMessageSource(MessageSourceConfig messageSourceConfig)
    {
        var connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(messageSourceConfig.ConnectionString),
        };
        _connection = connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();
        _queueName = messageSourceConfig.TopicOrQueueName;
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken = default)
    {
        if (_isConsuming)
        {
            return;
        }
        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = new MessageModel
                {
                    Content = System.Text.Encoding.UTF8.GetString(body)
                };

                if (OnMessageReceived is not null)
                {
                    var isSuccess = await OnMessageReceived(message);
                    if (isSuccess)
                    {
                        _channel.BasicAck(ea.DeliveryTag, false);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"处理RabbitMQ消息失败: {ex.Message}, 消息：{JsonSerializer.Serialize(ea)}");
                _channel.BasicNack(ea.DeliveryTag, false, true);
            }
        };

        var consumerTag = _channel.BasicConsume(_queueName, false, consumer);
        _isConsuming = true;
        await Task.CompletedTask;
    }

    public Task StopConsumingAsync()
    {
        _isConsuming = false;
        _channel?.Close();
        _connection?.Close();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }
}