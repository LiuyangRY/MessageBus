using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息目标配置模型
/// </summary>
public class RabbitMessageTargetConfig : MessageTargetConfig
{
    /// <summary>
    /// 虚拟主机
    /// </summary>
    public required string VirtualHost { get; set; }

    /// <summary>
    /// 目标通道类型
    /// </summary>
    public override required EnumChannelType TargetChannelType { get; set; } = EnumChannelType.RabbitMQ;

    /// <summary>
    /// 交换机名称
    /// </summary>
    public required string ExchangeName { get; set; }

    /// <summary>
    /// 路由键
    /// </summary>
    public required string RoutingKey { get; set; }
}