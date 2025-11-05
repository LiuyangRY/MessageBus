using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();

builder.Services.AddKeyedSingleton("UpstreamMQClient", (sp, key) =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    return new ConnectionFactory
    {
        Uri = new Uri(config.GetConnectionString("upstream-rabbitmq")!)
    };
});

builder.Services.AddKeyedSingleton("MessageBusMQClient", (sp, key) =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    return new ConnectionFactory
    {
        Uri = new Uri(config.GetConnectionString("messagebus-rabbitmq")!)
    };
});

builder.Services.AddKeyedSingleton("DownstreamMQClient", (sp, key) =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    return new ConnectionFactory
    {
        Uri = new Uri(config.GetConnectionString("downstream-rabbitmq")!)
    };
});

var app = builder.Build();

// 应用启动时确保所有队列存在
app.Lifetime.ApplicationStarted.Register(() =>
{
    using var scope = app.Services.CreateScope();
    var services = scope.ServiceProvider;
    
    var upstreamFactory = services.GetRequiredKeyedService<ConnectionFactory>("UpstreamMQClient");
    var centerFactory = services.GetRequiredKeyedService<ConnectionFactory>("MessageBusMQClient");
    var downstreamFactory = services.GetRequiredKeyedService<ConnectionFactory>("DownstreamMQClient");

    EnsureQueuesExist(upstreamFactory, centerFactory, downstreamFactory);
});

app.Run();

/// <summary>
/// 确保所有队列存在，如果不存在则创建
/// </summary>
void EnsureQueuesExist(ConnectionFactory upstreamFactory, ConnectionFactory centerFactory, ConnectionFactory downstreamFactory)
{
    try
    {
        Console.WriteLine("开始检查并创建队列...");
        
        EnsureUpstreamQueues(upstreamFactory);
        EnsureCenterQueues(centerFactory);
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
void EnsureUpstreamQueues(ConnectionFactory factory)
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
/// 创建或确保消息中心队列存在
/// </summary>
void EnsureCenterQueues(ConnectionFactory factory)
{
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();

    // 创建主交换机和队列
    CreateExchange(channel, "message-center.exchange", ExchangeType.Direct);
    
    var processQueueArgs = new Dictionary<string, object>
    {  
        {"x-dead-letter-exchange", "message-center.dlq.exchange"},
        {"x-dead-letter-routing-key", "dlq.process"}
    };
    
    CreateQueue(channel, "message-center.process.queue", 
        exchange: "message-center.exchange", 
        routingKey: "process",
        arguments: processQueueArgs);

    // 创建死信交换机和队列
    CreateExchange(channel, "message-center.dlq.exchange", ExchangeType.Direct);
    
    var dlqArgs = new Dictionary<string, object>
    {
        {"x-queue-mode", "lazy"}
    };
    
    CreateQueue(channel, "message-center.dlq", 
        exchange: "message-center.dlq.exchange", 
        routingKey: "dlq.process",
        arguments: dlqArgs);
    
    Console.WriteLine("消息中心队列已确保存在");
}

/// <summary>
/// 创建或确保下游队列存在
/// </summary>
void EnsureDownstreamQueues(ConnectionFactory factory)
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