namespace MessageBus.Service.Models;

/// <summary>
/// 通用消息模型
/// </summary>
public class MessageModel : BaseModel
{
    /// <summary>
    /// 消息源ID（用于消息中心路由）
    /// </summary>
    public long MessageSourceId { get; set; }

    /// <summary>
    /// 消息内容
    /// </summary>
    public required string Content { get; set; }
}