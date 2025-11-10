using Microsoft.Extensions.DependencyInjection;

var builder = DistributedApplication.CreateBuilder(args);

var apiService = builder.AddProject<Projects.MessageBus_ApiService>("apiservice");

builder.AddProject<Projects.MessageBus_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithReference(apiService);


var rabbitmqUserName = builder.AddParameter("rabbitmq-username", "admin");
var rabbitmqPassword = builder.AddParameter("rabbitmq-password", "admin123");
var upstreamRabbitmq = builder.AddRabbitMQ("upstream-rabbitmq", rabbitmqUserName, rabbitmqPassword)
    .WithImage("rabbitmq:management")
    .WithContainerName("upstreamRabbitmq")
    .WithLifetime(ContainerLifetime.Persistent)
    .WithExternalHttpEndpoints()
    .WithEnvironment("TZ", "Asia/Shanghai")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", rabbitmqUserName)
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", rabbitmqPassword)
    .WithEnvironment("RABBITMQ_LOOPBACK_USERS", "[]")
    .WithEndpoint("rabbit-management", endpoint =>
    {
        endpoint.Port = 24000;
        endpoint.TargetPort = 15672;
    });
var downstreamRabbitmq = builder.AddRabbitMQ("downstream-rabbitmq", rabbitmqUserName, rabbitmqPassword)
    .WithImage("rabbitmq:management")
    .WithContainerName("downstreamRabbitmq")
    .WithLifetime(ContainerLifetime.Persistent)
    .WithEnvironment("TZ", "Asia/Shanghai")
    .WithEnvironment("RABBITMQ_DEFAULT_USER", rabbitmqUserName)
    .WithEnvironment("RABBITMQ_DEFAULT_PASS", rabbitmqPassword)
    .WithEnvironment("RABBITMQ_LOOPBACK_USERS", "[]")
    .WithEndpoint("rabbit-management", endpoint =>
    {
        endpoint.Port = 26000;
        endpoint.TargetPort = 15672;
    });
var messageCenterKafka = builder.AddKafka("messagebus-kafka")
    .WithContainerName("messageBusKafka")
    .WithLifetime(ContainerLifetime.Persistent)
    .WithKafkaUI();
var messageBusService = builder.AddProject<Projects.MessageBus_Service>("messageservice")
    .WaitFor(upstreamRabbitmq)
    .WaitFor(downstreamRabbitmq)
    .WaitFor(messageCenterKafka)
    .WithReference(upstreamRabbitmq)
    .WithReference(downstreamRabbitmq)
    .WithReference(messageCenterKafka);
builder.Build().Run();
