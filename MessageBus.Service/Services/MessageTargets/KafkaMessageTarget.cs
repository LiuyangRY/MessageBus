using System.Text.Json;
using Confluent.Kafka;
using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Services.MessageTargets;

/// <summary>
/// Kafka消息目标实现
/// </summary>
public class KafkaMessageTarget : IMessageTarget
{
    private IProducer<Null, MessageModel>? _producer;

    public static EnumChannelType TargetChannelType => EnumChannelType.Kafka;

    private readonly string _topic;

    public bool IsMessageBusTarget { get; }

    public KafkaMessageTarget(KafkaMessageTargetConfig config)
    {
        IsMessageBusTarget = config.IsMessageBusTarget;
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.ConnectionString,
            Acks = Acks.All,
            MessageSendMaxRetries = 5,
            RetryBackoffMs = 1000
        };
        
        _producer = new ProducerBuilder<Null, MessageModel>(producerConfig)
            .SetValueSerializer(new MessageModelSerializer())
            .Build();
        _topic = config.Topic;
    }

    public async Task<bool> SendMessageAsync(MessageModel message)
    {
        try
        {
            var kafkaMessage = new Message<Null, MessageModel>
            {
                Value = message,
            };

            var deliveryResult = await _producer!.ProduceAsync(_topic, kafkaMessage);

            Console.WriteLine($"Kafka消息发送成功: 主题={deliveryResult.Topic}, 分区={deliveryResult.Partition}, 偏移量={deliveryResult.Offset}");

            return deliveryResult.Status == PersistenceStatus.Persisted;
        }
        catch (ProduceException<Null, string> ex)
        {
            Console.WriteLine($"Kafka消息发送失败: {ex.Error.Reason}, 消息：{JsonSerializer.Serialize(message)}");
            return false;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Kafka消息发送异常: {ex.Message}, 消息：{JsonSerializer.Serialize(message)}");
            return false;
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}

/// <summary>
/// MessageModel类型的JSON序列化器
/// </summary>
public class MessageModelSerializer : ISerializer<MessageModel>
{
    public byte[] Serialize(MessageModel data, SerializationContext context)
    {
        if (data is null)
        {
            return null!;
        }

        try
        {
            var jsonString = JsonSerializer.Serialize(data);
            return System.Text.Encoding.UTF8.GetBytes(jsonString);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"MessageModel序列化失败: {ex.Message}，消息：{data.Content}");
            throw;
        }
    }
}