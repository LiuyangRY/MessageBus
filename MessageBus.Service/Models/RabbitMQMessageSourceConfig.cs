using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息源配置模型
/// </summary>
public class RabbitMQMessageSourceConfig : MessageSourceConfig
{
    /// <summary>
    /// 消息源通道类型
    /// </summary>
    public override required EnumChannelType SourceChannelType { get; set; } = EnumChannelType.RabbitMQ;

    /// <summary>
    /// 队列名称
    /// </summary>
    public required string QueueName { get; set; }
}