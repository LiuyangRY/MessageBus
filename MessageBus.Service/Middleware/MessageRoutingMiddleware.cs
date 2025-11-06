using MessageBus.Service.Interfaces;
using MessageBus.Service.Services;

namespace MessageBus.Service.Middleware;

/// <summary>
/// 消息路由中间件
/// </summary>
public class MessageRoutingMiddleware
{
    private readonly List<IMessageSource> _sources = [];

    public MessageRoutingMiddleware(BindingManager bindingManager)
    {
        _sources = bindingManager.GetAllMessageSources();
    }

    /// <summary>
    /// 启动消息路由中间件
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>任务</returns>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        foreach (var source in _sources)
        {
            await source.StartConsumingAsync(cancellationToken);
        }
    }

    /// <summary>
    /// 停止消息路由中间件
    /// </summary>
    /// <returns>任务</returns>
    public async Task StopAsync()
    {
        foreach (var source in _sources)
        {
            await source.StopConsumingAsync();
        }
    }
}