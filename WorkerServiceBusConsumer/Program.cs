using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace WorkerServiceBusConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Azure Service Bus (Consumer) tests: ");
            Console.WriteLine("Type 1 to test Azure Service Bus + Queue");
            Console.WriteLine("Type 2 to test Azure Service Bus + Topic");

            var line = Console.ReadLine();

            int.TryParse(line, out var selectedOption);
            Console.WriteLine($"selected option: {selectedOption}");



            if (selectedOption != 1 && selectedOption != 2)
            {
                Console.WriteLine("Please, select the correct options and try again");
                return;
            }

            if (selectedOption == 1)
            {
                Console.WriteLine("Testing the messages consumption with Azure Service Bus + Queue");

                Console.WriteLine("Type the Azure Service Bus Connection String: ");
                var connectionString = Console.ReadLine();

                Console.WriteLine("Type the Queue name to be used on the messages comsumption: ");
                var queueName = Console.ReadLine();

                CreateHostBuilderQueue(connectionString, queueName).Build().Run();
            }
            else
            {
                Console.WriteLine("Testing the messages consumption with Azure Service Bus + Topics");

                Console.WriteLine("Type the Azure Service Bus Connection String: ");
                var connectionString = Console.ReadLine();

                Console.WriteLine("Type the Topic name to be used on the messages comsumption: ");
                var topicName = Console.ReadLine();

                Console.WriteLine("Type the application Subscription: ");
                var subscription = Console.ReadLine();

                CreateHostBuilderTopic(connectionString, topicName, subscription).Build().Run();
            }
        }

        public static IHostBuilder CreateHostBuilderQueue(string connectionString, string queueName) =>
            Host.CreateDefaultBuilder()
        .ConfigureServices((hostContext, services) =>
        {
            services.AddSingleton<Queue.ExecutionParameters>(
                new Queue.ExecutionParameters()
                {
                    ConnectionString = connectionString,
                    Queue = queueName
                });
            services.AddHostedService<Queue.Worker>();
        });

        public static IHostBuilder CreateHostBuilderTopic(string connectionString, string topicName, string subscription) =>
            Host.CreateDefaultBuilder()
        .ConfigureServices((hostContext, services) =>
        {
            services.AddSingleton<Topic.ExecutionParameters>(
                new Topic.ExecutionParameters()
                {
                    ConnectionString = connectionString,
                    Topic = topicName,
                    Subscription = subscription
                });
            services.AddHostedService<Topic.Worker>();
        });
    }
}
