namespace MessageBus.Service.Models;

/// <summary>
/// 消息源-目标绑定关系模型
/// </summary>
public class SourceTargetBinding : BaseModel
{
    /// <summary>
    /// 消息源ID
    /// </summary>
    public required long SourceId { get; set; }

    /// <summary>
    /// 目标ID
    /// </summary>
    public required long TargetId { get; set; }

    /// <summary>
    /// 是否启用
    /// </summary>
    public bool Enabled { get; set; } = true;
}