using MessageBus.Service.Interfaces;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;
using MessageBus.Service.Services.MessageSources;
using MessageBus.Service.Services.MessageTargets;

namespace MessageBus.Service.Services;

/// <summary>
/// 绑定关系管理器
/// </summary>
public class BindingManager
{
    private readonly Dictionary<long, MessageSourceConfig> _sourceConfigs;
    private readonly Dictionary<long, MessageTargetConfig> _targetConfigs;
    private readonly Dictionary<long, List<SourceTargetBinding>> _bindings;
    private readonly Dictionary<long, IMessageSource> _messageSources;
    private readonly Dictionary<long, IMessageTarget> _messageTargets;

    public BindingManager()
    {
        _sourceConfigs = [];
        _targetConfigs = [];
        _bindings = [];
        _messageSources = [];
        _messageTargets = [];
    }

    /// <summary>
    /// 添加消息源配置
    /// </summary>
    public void AddSourceConfig(MessageSourceConfig config)
    {
        var messageSource = BuildMessageSource(config);
        _sourceConfigs[config.Id] = config;
        _messageSources[config.Id] = messageSource;
    }

    /// <summary>
    /// 添加消息目标配置
    /// </summary>
    public void AddTargetConfig(MessageTargetConfig config)
    {
        var messageTarget = BuildMessageTarget(config);
        _targetConfigs[config.Id] = config;
        _messageTargets[config.Id] = messageTarget;
    }

    /// <summary>
    /// 添加绑定关系
    /// </summary>
    public void AddBinding(SourceTargetBinding binding)
    {
        if (!_bindings.TryGetValue(binding.SourceId, out var bindingList))
        {
            bindingList = [];
            _bindings[binding.SourceId] = bindingList;
        }
        bindingList.Add(binding);
    }

    /// <summary>
    /// 获取所有消息源
    /// </summary>
    /// <returns>所有消息源列表</returns>
    public List<IMessageSource> GetAllMessageSources()
    {
        return [.. _messageSources.Values];
    }

    /// <summary>
    /// 根据消息源ID获取绑定的目标配置
    /// </summary>
    public List<MessageTargetConfig> GetTargetsBySourceId(long sourceId)
    {
        var result = new List<MessageTargetConfig>();
        _bindings.TryGetValue(sourceId, out var sourcebindings);
        if (sourcebindings?.Any() != true)
        {
            return result;
        }
        foreach (var binding in sourcebindings)
        {
            if (!binding.Enabled)
            {
                continue;
            }
            if (_targetConfigs.TryGetValue(binding.TargetId, out var targetConfig) && binding.Enabled)
            {
                result.Add(targetConfig);
            }
        }
        return result;
    }

    /// <summary>
    /// 构建消息源
    /// </summary>
    /// <param name="config">消息源配置</param>
    /// <returns>消息源实例</returns>
    private IMessageSource BuildMessageSource(MessageSourceConfig config)
    {
        switch (config.SourceChannelType)
        {
            case EnumChannelType.RabbitMQ:
                var messageSource = new RabbitMQMessageSource(config);
                messageSource.OnMessageReceived += async (message) =>
                {
                    var targets = GetTargetsBySourceId(config.Id);
                    foreach (var target in targets)
                    {
                        var messageTarget = _messageTargets[target.Id];
                        await messageTarget.SendMessageAsync(message);
                    }
                    return true;
                };
                return messageSource;
            default:
                throw new NotSupportedException($"不支持的消息源类型: {config.SourceChannelType}");
        }
    }

    /// <summary>
    /// 构建消息目标
    /// </summary>
    /// <param name="config">消息目标配置</param>
    /// <returns>消息目标实例</returns>
    private IMessageTarget BuildMessageTarget(MessageTargetConfig config)
    {
        switch (config.TargetChannelType)
        {
            case EnumChannelType.RabbitMQ:
                return new RabbitMQMessageTarget(config);
            default:
                throw new NotSupportedException($"不支持的消息目标类型: {config.TargetChannelType}");
        }
    }
}