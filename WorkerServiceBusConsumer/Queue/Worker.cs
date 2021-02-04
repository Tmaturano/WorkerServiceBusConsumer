using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WorkerServiceBusConsumer.Queue
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ExecutionParameters _executionParameters;
        private readonly QueueClient _client;

        public Worker(ILogger<Worker> logger,
            ExecutionParameters executionParameters)
        {
            logger.LogInformation($"Queue = {executionParameters.Queue}");

            _logger = logger;
            _executionParameters = executionParameters;
            _client = new QueueClient(_executionParameters.ConnectionString, _executionParameters.Queue,
                ReceiveMode.ReceiveAndDelete);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Run(() =>
                {
                    _logger.LogInformation("Starting the messages processing...");
                    _client.RegisterMessageHandler(
                        async (message, stoppingToken) =>
                        {
                            await ProcessMessages(message);
                        },
                        new MessageHandlerOptions(
                            async (e) =>
                            {
                                await ProcessError(e);
                            }));
                });
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _client.CloseAsync();
            _logger.LogInformation("Connection with Azure Service Bus is closed!");
        }

        private Task ProcessMessages(Message message)
        {
            var conteudo = Encoding.UTF8.GetString(message.Body);

            //it can be deserialized, apply business rules, integrate with other services, databases...
            _logger.LogInformation("[New message received]: " + conteudo);
            return Task.CompletedTask;
        }

        private Task ProcessError(ExceptionReceivedEventArgs e)
        {
            _logger.LogError("[Fail] " +
                e.Exception.GetType().FullName + " " +
                e.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
