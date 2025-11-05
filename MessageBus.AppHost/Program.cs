var builder = DistributedApplication.CreateBuilder(args);

var apiService = builder.AddProject<Projects.MessageBus_ApiService>("apiservice");

builder.AddProject<Projects.MessageBus_Web>("webfrontend")
    .WithExternalHttpEndpoints()
    .WithReference(apiService);

builder.Build().Run();
