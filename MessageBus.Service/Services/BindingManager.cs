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
    private IMessageSource BuildMessageSource(MessageSourceConfig config)
    {
        IMessageSource result = config.SourceChannelType switch
        {
            EnumChannelType.RabbitMQ => new RabbitMQMessageSource((RabbitMQMessageSourceConfig)config),
            EnumChannelType.Kafka => new KafkaMessageSource((KafkaMessageSourceConfig)config),
            EnumChannelType.RocketMQ => new RocketMQMessageSource((RocketMQMessageSourceConfig)config),
            _ => throw new NotSupportedException($"不支持的消息源类型: {config.SourceChannelType}"),
        };
        result.OnMessageReceived += async (message) =>
        {
            // 判断消息来源：如果MessageSourceId为空，说明是来自业务的消息源
            // 如果MessageSourceId不为空，说明是来自消息中心的消息
            if (message.MessageSourceId == 0)
            {
                // 来自业务消息源：设置MessageSourceId并发送到消息中心
                message.MessageSourceId = config.Id;
                
                // 查找消息中心生产者（Kafka目标）
                var messageCenterTarget = _messageTargets.Values.FirstOrDefault(config => config.IsMessageBusTarget);
                if (messageCenterTarget != null)
                {
                    await messageCenterTarget.SendMessageAsync(message);
                }
            }
            else
            {
                // 来自消息中心：直接获取绑定的目标进行转发
                var targets = GetTargetsBySourceId(message.MessageSourceId);
                foreach (var target in targets)
                {
                    var messageTarget = _messageTargets[target.Id];
                    await messageTarget.SendMessageAsync(message);
                }
            }
            return true;
        };
        return result;
    }

    /// <summary>
    /// 构建消息目标
    /// </summary>
    private static IMessageTarget BuildMessageTarget(MessageTargetConfig config)
    {
        return config.TargetChannelType switch
        {
            EnumChannelType.RabbitMQ => new RabbitMQMessageTarget((RabbitMessageTargetConfig)config),
            EnumChannelType.Kafka => new KafkaMessageTarget((KafkaMessageTargetConfig)config),
            EnumChannelType.RocketMQ => new RocketMQMessageTarget((RocketMQMessageTargetConfig)config),
            _ => throw new NotSupportedException($"不支持的消息目标类型: {config.TargetChannelType}"),
        };
    }
}