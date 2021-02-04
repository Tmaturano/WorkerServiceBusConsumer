using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkerServiceBusConsumer.Topic
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ExecutionParameters _executionParameters;
        private readonly ISubscriptionClient _client;
        private readonly string _subscription;

        public Worker(ILogger<Worker> logger,
            ExecutionParameters executionParameters)
        {
            _logger = logger;
            _executionParameters = executionParameters;
            _subscription = executionParameters.Subscription;
            var topicName = executionParameters.Topic;

            _client = new SubscriptionClient(executionParameters.ConnectionString,
                topicName, _subscription);

            _logger.LogInformation($"Topic = {topicName}");
            _logger.LogInformation($"Subscription = {_subscription}");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    _logger.LogInformation("Waiting for messages...");
                    RegisterOnMessageHandlerAndReceiveMessages();
                });
            }
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {
            await _client.CloseAsync();
            _logger.LogInformation(
                "Azure Service Bus connection was closed!");
        }

        private void RegisterOnMessageHandlerAndReceiveMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(
                ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _client.RegisterMessageHandler(
                ProcessMessagesAsync, messageHandlerOptions);
        }

        private async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            _logger.LogInformation($"[{_subscription} | New message] " +
                Encoding.UTF8.GetString(message.Body));
            await _client.CompleteAsync(
                message.SystemProperties.LockToken);
        }

        private Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            _logger.LogError($"Message handler - Try - Exception: {exceptionReceivedEventArgs.Exception}.");

            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            _logger.LogError("Exception context - information to the problem solving:");
            _logger.LogError($"- Endpoint: {context.Endpoint}");
            _logger.LogError($"- Entity Path: {context.EntityPath}");
            _logger.LogError($"- Executing Action: {context.Action}");

            return Task.CompletedTask;
        }
    }
}
