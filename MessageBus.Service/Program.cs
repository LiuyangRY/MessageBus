using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using MessageBus.Service.Services;
using MessageBus.Service.Services.MessageTargets;
using MessageBus.Service.Middleware;
using MessageBus.Service.Models;
using MessageBus.Service.Models.Enums;
using MessageBus.Service.Interfaces;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

builder.AddKeyedRabbitMQClient("upstream-rabbitmq");
builder.AddKeyedRabbitMQClient("downstream-rabbitmq");

// 注册消息路由服务
builder.Services.AddSingleton<MessageRoutingMiddleware>();
builder.Services.AddSingleton<BindingManager>();
builder.Services.AddKeyedSingleton<IMessageTarget, RabbitMQMessageTarget>(EnumChannelType.RabbitMQ);

var app = builder.Build();

// 应用启动时初始化消息路由系统
app.Lifetime.ApplicationStarted.Register(async () =>
{
    using var scope = app.Services.CreateScope();
    var services = scope.ServiceProvider;
    
    // 确保队列存在
    var upstreamFactory = services.GetRequiredKeyedService<IConnectionFactory>("upstream-rabbitmq");
    var downstreamFactory = services.GetRequiredKeyedService<IConnectionFactory>("downstream-rabbitmq");

    EnsureQueuesExist(upstreamFactory, downstreamFactory);

    var config = services.GetRequiredService<IConfiguration>();
    
    // ==================== 消息中心Kafka配置 ====================
    
    // 1. 配置消息中心Kafka消息消费者（接收所有业务消息源的消息）
    var kafkaMessageBusConsumer = new KafkaMessageSourceConfig
    {
        Id = 1001,
        Name = "消息中心Kafka消息消费者",
        Code = "message-bus-kafka-consumer",
        GroupId = "message-bus-group",
        SourceChannelType = EnumChannelType.Kafka,
        ConnectionString = config.GetConnectionString("messagebus-kafka")!,
        TopicName = "message-bus",
        Enabled = true
    };
    
    // 2. 配置消息中心Kafka消息生产者（处理消息并路由到具体目标）
    var kafkaMessageBusProducer = new KafkaMessageTargetConfig
    {
        IsMessageBusTarget = true,
        Id = 2001,
        Name = "消息中心Kafka消息生产者",
        Code = "message-bus-kafka-producer",
        TargetChannelType = EnumChannelType.Kafka,
        ConnectionString = config.GetConnectionString("messagebus-kafka")!,
        Topic = "message-bus",
        Enabled = true
    };
    
    // ==================== 业务消息源配置 ====================
    
    // 3. 配置业务RabbitMQ消息源（上游业务系统）
    var rabbitmqBizSource = new RabbitMQMessageSourceConfig
    {
        Id = 1002,
        Name = "业务系统RabbitMQ消息源",
        Code = "biz-rabbitmq-source",
        SourceChannelType = EnumChannelType.RabbitMQ,
        ConnectionString = config.GetConnectionString("upstream-rabbitmq")!,
        QueueName = "upstream.queue",
        Enabled = true
    };
    
    // ==================== 具体业务目标配置 ====================
    
    // 5. 配置订单业务RabbitMQ目标
    var rabbitmqOrderTarget = new RabbitMessageTargetConfig
    {
        Id = 2002,
        VirtualHost = "/",
        Name = "订单业务RabbitMQ目标",
        Code = "order-rabbitmq-target",
        TargetChannelType = EnumChannelType.RabbitMQ,
        ConnectionString = config.GetConnectionString("downstream-rabbitmq")!,
        ExchangeName = "downstream.exchange",
        RoutingKey = "order.routingkey",
        Enabled = true
    };
    
    // 6. 配置库存业务RabbitMQ目标
    var rabbitmqInventoryTarget = new RabbitMessageTargetConfig
    {
        Id = 2003,
        VirtualHost = "/",
        Name = "库存业务RabbitMQ目标",
        Code = "inventory-rabbitmq-target",
        TargetChannelType = EnumChannelType.RabbitMQ,
        ConnectionString = config.GetConnectionString("downstream-rabbitmq")!,
        ExchangeName = "downstream.exchange",
        RoutingKey = "inventory.routingkey",
        Enabled = true
    };

    // ==================== 绑定关系配置 ====================
    
    var bindings = new List<SourceTargetBinding>
    {
        // 消息中心Kafka消息生产者 -> 消息中心Kafka消息消费者（消息中心处理后路由到具体目标）
        new() {
            Id = 3003,
            SourceId = kafkaMessageBusProducer.Id, // 消息中心Kafka消息生产者
            TargetId = kafkaMessageBusConsumer.Id, // 消息中心Kafka消息消费者
            Enabled = true,
        },
        // 业务消息源 -> 业务消息目标（业务消息都发送到具体业务目标）
        new() {
            Id = 3001,
            SourceId = rabbitmqBizSource.Id,        // 业务RabbitMQ源
            TargetId = rabbitmqOrderTarget.Id,      // 订单业务目标
            Enabled = true,
        },
        new() {
            Id = 3002,
            SourceId = rabbitmqBizSource.Id,        // 业务RabbitMQ源
            TargetId = rabbitmqInventoryTarget.Id,  // 库存业务目标
            Enabled = true,
        },
    };
    
    var bindingManager = services.GetRequiredService<BindingManager>();
    
    // 注册消息中心配置
    bindingManager.AddSourceConfig(kafkaMessageBusConsumer);
    bindingManager.AddTargetConfig(kafkaMessageBusProducer);
    
    // 注册业务消息源配置
    bindingManager.AddSourceConfig(rabbitmqBizSource);
    
    // 注册业务目标配置
    bindingManager.AddTargetConfig(rabbitmqOrderTarget);
    bindingManager.AddTargetConfig(rabbitmqInventoryTarget);
    
    // 注册绑定关系
    foreach (var binding in bindings)
    {
        bindingManager.AddBinding(binding);
    }

    // 启动消息路由
    var routingMiddleware = services.GetRequiredService<MessageRoutingMiddleware>();
    await routingMiddleware.StartAsync();
});

app.Lifetime.ApplicationStopping.Register(async () =>
{
    using var scope = app.Services.CreateScope();
    var routingMiddleware = scope.ServiceProvider.GetRequiredService<MessageRoutingMiddleware>();
    await routingMiddleware.StopAsync();
});

app.Run();

/// <summary>
/// 确保所有队列存在，如果不存在则创建
/// </summary>
void EnsureQueuesExist(IConnectionFactory upstreamFactory, IConnectionFactory downstreamFactory)
{
    try
    {
        Console.WriteLine("开始检查并创建队列...");
        
        EnsureUpstreamQueues(upstreamFactory);
        EnsureDownstreamQueues(downstreamFactory);
        
        Console.WriteLine("所有队列检查完成");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"队列创建失败: {ex.Message}");
        throw;
    }
}

/// <summary>
/// 创建或确保上游队列存在
/// </summary>
void EnsureUpstreamQueues(IConnectionFactory factory)
{
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();

    // 创建上游交换机
    CreateExchange(channel, "upstream.exchange", ExchangeType.Topic);
    
    // 创建上游队列
    CreateQueue(channel, "upstream.queue", 
        exchange: "upstream.exchange", 
        routingKey: "#");
    
    Console.WriteLine("上游队列已确保存在");
}

/// <summary>
/// 创建或确保下游队列存在
/// </summary>
void EnsureDownstreamQueues(IConnectionFactory factory)
{
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();

    // 创建下游交换机
    CreateExchange(channel, "downstream.exchange", ExchangeType.Topic);
    
    // 创建订单队列
    CreateQueue(channel, "downstream.order.queue", 
        exchange: "downstream.exchange", 
        routingKey: "order.#");
    
    // 创建库存队列
    CreateQueue(channel, "downstream.inventory.queue", 
        exchange: "downstream.exchange", 
        routingKey: "inventory.#");
    
    Console.WriteLine("下游队列已确保存在");
}

/// <summary>
/// 创建交换机的通用方法
/// </summary>
void CreateExchange(IModel channel, string exchangeName, string exchangeType)
{
    try
    {
        // 尝试声明交换机，如果已存在会成功（幂等操作）
        channel.ExchangeDeclare(
            exchange: exchangeName,
            type: exchangeType,
            durable: true,      // 持久化
            autoDelete: false,  // 不自动删除
            arguments: null);
        
        Console.WriteLine($"交换机 '{exchangeName}' 已确保存在");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"创建交换机 '{exchangeName}' 失败: {ex.Message}");
        throw;
    }
}

/// <summary>
/// 创建队列的通用方法（改进版本，避免通道关闭问题）
/// </summary>
void CreateQueue(IModel channel, string queueName, string exchange, string routingKey, 
    IDictionary<string, object>? arguments = null)
{
    try
    {
        // 直接声明队列（幂等操作）
        channel.QueueDeclare(
            queue: queueName,
            durable: true,      // 持久化
            exclusive: false,   // 非独占
            autoDelete: false,  // 不自动删除
            arguments: arguments);
        
        Console.WriteLine($"队列 '{queueName}' 已确保存在");

        // 绑定队列到交换机
        channel.QueueBind(
            queue: queueName,
            exchange: exchange,
            routingKey: routingKey);
        
        Console.WriteLine($"队列 '{queueName}' 已绑定到交换机 '{exchange}'");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"处理队列 '{queueName}' 失败: {ex.Message}");
        throw;
    }
}