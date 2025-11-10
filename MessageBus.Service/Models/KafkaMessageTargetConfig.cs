using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息目标配置模型
/// </summary>
public class KafkaMessageTargetConfig : MessageTargetConfig
{
    /// <summary>
    /// 目标通道类型
    /// </summary>
    public override required EnumChannelType TargetChannelType { get; set; } = EnumChannelType.Kafka;

    /// <summary>
    /// 主题名称
    /// </summary>
    public required string Topic { get; set; }
}