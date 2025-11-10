using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息源配置模型
/// </summary>
public class KafkaMessageSourceConfig : MessageSourceConfig
{
    /// <summary>
    /// 消息源通道类型
    /// </summary>
    public override required EnumChannelType SourceChannelType { get; set; } = EnumChannelType.Kafka;

    /// <summary>
    /// 消费者组ID
    /// </summary>
    public required string GroupId { get; set; }

    /// <summary>
    /// 主题名称
    /// </summary>
    public required string TopicName { get; set; }
}