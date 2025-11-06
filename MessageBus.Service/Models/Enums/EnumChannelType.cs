namespace MessageBus.Service.Models.Enums;

/// <summary>
/// 通道类型枚举 
/// </summary>
public enum EnumChannelType
{
    /// <summary>
    /// RabbitMQ通道
    /// </summary>
    RabbitMQ = 1,

    /// <summary>
    /// RocketMQ通道
    /// </summary>
    RocketMQ = 2,

    /// <summary>
    /// Kafka通道
    /// </summary>
    Kafka = 4,

    /// <summary>
    /// API通道
    /// </summary>
    API = 8
}
