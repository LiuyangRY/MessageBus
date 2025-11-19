using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// RocketMQ消息目标配置模型
/// </summary>
public class RocketMQMessageTargetConfig : MessageTargetConfig
{
    /// <summary>
    /// 消息目标通道类型
    /// </summary>
    public override required EnumChannelType TargetChannelType { get; set; } = EnumChannelType.RocketMQ;

    /// <summary>
    /// 主题名称
    /// </summary>
    public required string Topic { get; set; }

    /// <summary>
    /// 标签（可选）
    /// </summary>
    public string? Tag { get; set; }

    /// <summary>
    /// 消息密钥（可选）
    /// </summary>
    public string? MessageKey { get; set; }
}