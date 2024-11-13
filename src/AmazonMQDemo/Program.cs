using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using StackExchange.Redis;

namespace RabbitMQConsoleApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var builder = new HostBuilder()
                .ConfigureAppConfiguration((hostingContext, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                });

            var host = builder.Build();
            var configuration = host.Services.GetRequiredService<IConfiguration>();

            //IConnection connection;
            //IModel channel;
            // TestRabbitMQ(configuration, out connection, out channel);
            await TestRedis(configuration);
            await host.RunAsync();
        }

        private static async Task TestRedis(IConfiguration Configuration)
        {
            // Get Redis connection settings
            try
            {
                string endpoint = Configuration["RedisCache:Endpoint"];
                int port = int.Parse(Configuration["RedisCache:Port"]);
                string username = Configuration["RedisCache:Username"];
                string password = Configuration["RedisCache:Password"];

                // Create the connection string for Redis
                var options = new ConfigurationOptions
                {
                    EndPoints = { $"{endpoint}:{port}" },
                    User = username, // Optional: Only use if Redis authentication is enabled with a username
                    Password = password,
                    Ssl = true,  // Recommended for AWS ElastiCache
                    AbortOnConnectFail = false,
                    ConnectTimeout = 15000
                };

                // Connect to Redis
                using var connection = ConnectionMultiplexer.Connect(endpoint); // ConnectionMultiplexer.Connect("localhost:6379");  //ConnectionMultiplexer.Connect(options);
                connection.ConnectionFailed += Connection_ConnectionFailed;

                Console.WriteLine("Mass insert (async/pipelined) test...");
                await MassInsertAsync(connection);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }

        private static void Connection_ConnectionFailed(object? sender, ConnectionFailedEventArgs e)
        {
            Console.WriteLine($"CONNECTION FAILED: {e.ConnectionType}, {e.FailureType}, {e.Exception}");
        }

        private static async Task MassInsertAsync(ConnectionMultiplexer connection)
        {
            const int NUM_INSERTIONS = 10;
            const int BATCH = 1;
            int matchErrors = 0;

            var database = connection.GetDatabase(0);

            var outstanding = new List<(Task, Task<RedisValue>, string)>(BATCH);

            for (int i = 0; i < NUM_INSERTIONS; i++)
            {
                var key = $"StackExchange.Redis.Test.{i}";
                var value = i.ToString();

                var set = database.StringSetAsync(key, value);
                var get = database.StringGetAsync(key);

                outstanding.Add((set, get, value));

                if (i > 0 && i % BATCH == 0)
                {
                    matchErrors += await ValidateAsync(outstanding);
                    Console.WriteLine(i);
                }
            }

            matchErrors += await ValidateAsync(outstanding);

            Console.WriteLine($"Match errors: {matchErrors}");

            static async Task<int> ValidateAsync(List<(Task, Task<RedisValue>, string)> outstanding)
            {
                int matchErrors = 0;
                foreach (var row in outstanding)
                {
                    var s = await row.Item2;
                    await row.Item1;
                    if (s != row.Item3)
                    {
                        matchErrors++;
                    }
                }
                outstanding.Clear();
                return matchErrors;
            }
        }

        private static void TestRabbitMQ(IConfiguration configuration, out IConnection connection, out IModel channel)
        {
            // Retrieve RabbitMQ configuration from appsettings.json
            var rabbitMqSettings = configuration.GetSection("RabbitMQ");
            var username = rabbitMqSettings["Username"];
            var password = rabbitMqSettings["Password"];
            var address = rabbitMqSettings["Address"];

            // Create a connection to RabbitMQ
            var factory = new ConnectionFactory
            {
                Uri = new Uri(address),
                UserName = username,
                Password = password
            };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
            Console.WriteLine("Connected to RabbitMQ.");

            // Example: Publish a message to a queue
            var queueName = "testQueue";
            channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
            var message = "Hello RabbitMQ!";
            var body = Encoding.UTF8.GetBytes(message);

            channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);
            Console.WriteLine($"Sent message: {message}");
        }
    }
}
