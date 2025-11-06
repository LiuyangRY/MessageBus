using MessageBus.Service.Models;

namespace MessageBus.Service.Interfaces;

/// <summary>
/// 消息目标接口
/// </summary>
public interface IMessageTarget
{
    /// <summary>
    /// 发送消息
    /// </summary>
    /// <param name="message">要发送的消息模型</param>
    /// <returns>是否发送成功</returns>
    Task<bool> SendMessageAsync(MessageModel message);
}