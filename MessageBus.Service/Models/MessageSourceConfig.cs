using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Models;

/// <summary>
/// 消息源配置模型
/// </summary>
public class MessageSourceConfig : BaseModel
{
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
    /// 消息源通道类型
    /// </summary>
    public required EnumChannelType SourceChannelType { get; set; }

    /// <summary>
    /// 连接字符串或配置
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    /// 主题名称或队列名称
    /// </summary>
    public required string TopicOrQueueName { get; set; }

    /// <summary>
    /// 是否启用
    /// </summary>
    public bool Enabled { get; set; }
}