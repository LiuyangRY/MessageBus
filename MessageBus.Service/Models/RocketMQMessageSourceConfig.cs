using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// RocketMQ消息源配置模型
/// </summary>
public class RocketMQMessageSourceConfig : MessageSourceConfig
{
    /// <summary>
    /// 消息源通道类型
    /// </summary>
    public override required EnumChannelType SourceChannelType { get; set; } = EnumChannelType.RocketMQ;

    /// <summary>
    /// 消费者组ID
    /// </summary>
    public required string ConsumerGroup { get; set; }

    /// <summary>
    /// 主题名称
    /// </summary>
    public required string TopicName { get; set; }

    /// <summary>
    /// 标签（可选）
    /// </summary>
    public string? Tag { get; set; }
}