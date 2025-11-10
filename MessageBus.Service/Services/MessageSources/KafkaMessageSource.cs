using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Services.MessageSources;

/// <summary>
/// Kafka消息源实现
/// </summary>
public class KafkaMessageSource : IMessageSource
{
    private readonly IConsumer<Ignore, MessageModel>? _consumer;
    private bool _isConsuming = false;
    private readonly string _bootstrapServers;
    private readonly string _topicName;

    public EnumChannelType SourceChannelType => EnumChannelType.Kafka;

    public event Func<MessageModel, Task<bool>>? OnMessageReceived;
    private Task? _consumingTask;

    public KafkaMessageSource(KafkaMessageSourceConfig config)
    {
        _bootstrapServers = config.ConnectionString;
        _topicName = config.TopicName;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = config.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            RetryBackoffMs = 1000,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true
        };
        _consumer = new ConsumerBuilder<Ignore, MessageModel>(consumerConfig)
            .SetValueDeserializer(new MessageModelDeserializer())
            .Build();
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken = default)
    {
        if (_isConsuming)
            return;

        await EnsureTopicExistsAsync();

        _consumer!.Subscribe(_topicName);
        _isConsuming = true;
        _consumingTask = Task.Run(() => ConsumeMessagesAsync(cancellationToken), cancellationToken);
        await Task.CompletedTask;
    }

    /// <summary>
    /// 确保主题存在，如果不存在则创建
    /// </summary>
    private async Task EnsureTopicExistsAsync()
    {
        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _bootstrapServers
            }).Build();

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var topicExists = metadata.Topics.Any(t => t.Topic == _topicName);

            if (!topicExists)
            {
                Console.WriteLine($"主题 '{_topicName}' 不存在，正在创建...");

                await adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = _topicName,
                        NumPartitions = 3,
                        ReplicationFactor = 1
                    }
                ]);

                Console.WriteLine($"主题 '{_topicName}' 创建成功");
            }
            else
            {
                Console.WriteLine($"主题 '{_topicName}' 已存在");
            }
        }
        catch (CreateTopicsException ex)
        {
            if (ex.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
            {
                Console.WriteLine($"主题 '{_topicName}' 已存在");
            }
            else
            {
                Console.WriteLine($"创建主题 '{_topicName}' 失败: {ex.Results[0].Error.Reason}");
                throw;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"检查主题 '{_topicName}' 时发生错误: {ex.Message}");
            // 不抛出异常，让消费者继续尝试订阅
        }
    }

    private async Task ConsumeMessagesAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested && _isConsuming)
        {
            try
            {
                var consumeResult = _consumer!.Consume(cancellationToken);
                if (consumeResult?.Message?.Value is not null && OnMessageReceived is not null)
                {
                    await OnMessageReceived(consumeResult.Message.Value);
                }
                _consumer.Commit(consumeResult);
            }
            catch (ConsumeException ex)
            {
            if (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                Console.WriteLine($"主题 '{_topicName}' 不存在，等待5秒后重试...");
                await Task.Delay(5000, cancellationToken);
                await EnsureTopicExistsAsync();  // 重新尝试创建主题
            }
            else
            {
                Console.WriteLine($"Kafka消费错误: {ex.Error.Reason}");
            }
        }
            catch (OperationCanceledException)
            {
            // 正常取消，不记录错误
            break;
        }
            catch (Exception ex)
            {
            Console.WriteLine($"Kafka消息处理异常: {ex.Message}");
            await Task.Delay(1000, cancellationToken);  // 错误后等待1秒再重试
        }
    }
    }

    public async Task StopConsumingAsync()
    {
        _isConsuming = false;
        if (_consumingTask is not null)
        {
            await _consumingTask;
        }
        _consumer?.Close();
        _consumer?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await StopConsumingAsync();
    }
}

/// <summary>
/// MessageModel类型的JSON反序列化器
/// </summary>
public class MessageModelDeserializer : IDeserializer<MessageModel>
{
    public MessageModel Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
        {
            return null!;
        }

        try
        {
            var jsonString = System.Text.Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<MessageModel>(jsonString) ?? new MessageModel
            {
                MessageSourceId = 0,
                Content = jsonString
            };
        }
        catch (JsonException)
        {
            // 如果JSON反序列化失败，将原始内容作为消息内容
            var content = System.Text.Encoding.UTF8.GetString(data);
            return new MessageModel
            {
                MessageSourceId = 0,
                Content = content
            };
        }
        catch (Exception ex)
        {
            var errorMessage = $"反序列化失败: {ex.Message}，消息：{data.ToString()}";
            Console.WriteLine(errorMessage);
            return new MessageModel
            {
                MessageSourceId = 0,
                Content = errorMessage
            };
        }
    }
}