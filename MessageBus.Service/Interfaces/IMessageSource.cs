using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;

namespace MessageBus.Service.Interfaces;

/// <summary>
/// 消息源接口
/// </summary>
public interface IMessageSource
{
    /// <summary>
    /// 消息来源通道类型 
    /// </summary>
    EnumChannelType SourceChannelType { get; }

    /// <summary>
    /// 开始消费消息
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>任务</returns>
    Task StartConsumingAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 停止消费消息
    /// </summary>
    /// <returns>任务</returns>
    Task StopConsumingAsync();

    /// <summary>
    /// 消息接收事件
    /// </summary>
    event Func<MessageModel, Task<bool>> OnMessageReceived;
}
