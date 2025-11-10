using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息目标配置模型
/// </summary>
public class MessageTargetConfig : BaseModel
{
    /// <summary>
    /// 是否是消息总线目标
    /// </summary>
    public bool IsMessageBusTarget { get; set; }

    /// <summary>
    /// 消息源名称
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// 消息源编码
    /// </summary>
    public required string Code { get; set; }

    /// <summary>
    /// 消息源描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 目标通道类型
    /// </summary>
    public virtual required EnumChannelType TargetChannelType { get; set; }

    /// <summary>
    /// 连接字符串或配置
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    /// 是否启用
    /// </summary>
    public bool Enabled { get; set; }
}