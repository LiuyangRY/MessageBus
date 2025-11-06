using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using RabbitMQ.Client;

namespace MessageBus.Service.Services.MessageTargets;

public class RabbitMQMessageTarget : IMessageTarget
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly string _exchangeName;
    private readonly string _routingKey;

    public RabbitMQMessageTarget(MessageTargetConfig targetConfig)
    {
        var connectionFactory = new ConnectionFactory
        {
            Uri = new Uri(targetConfig.ConnectionString),
        };
        _connection = connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();
        _exchangeName = targetConfig.ExchangeOrTopic;
        _routingKey = targetConfig.RoutingKey;
    }

    public async Task<bool> SendMessageAsync(MessageModel message)
    {
        try
        {
            _channel.ExchangeDeclare(exchange: _exchangeName, type: ExchangeType.Topic, durable: true);
            var body = System.Text.Encoding.UTF8.GetBytes(message.Content);
            var properties = _channel.CreateBasicProperties();
            properties.Persistent = true;

            _channel.BasicPublish(
                exchange: _exchangeName,
                routingKey: _routingKey,
                basicProperties: properties,
                body: body);

            return await Task.FromResult(true);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"发送消息到RabbitMQ失败: {ex.Message}");
            return await Task.FromResult(false);
        }
    }
}