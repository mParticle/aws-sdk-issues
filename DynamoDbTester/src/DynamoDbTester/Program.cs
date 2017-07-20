using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDbTester
{
    public class Program
    {
        static readonly CancellationTokenSource cts = new CancellationTokenSource();
        static AmazonDynamoDBClient client;
        static string tableName;
        static long RequestCount = 0;
        static int taskDelay;
        static int requestBatching = 1;

        public static void Main(string[] args)
        {
            if ((args?.Length ?? 0) < 2)
            {
                Console.WriteLine("Requires 2 arguments, <table name> and <write delay>.  Optionally also <num concurrent tasks> <num requests per batch>.");
                return;
            }

            var config = new AmazonDynamoDBConfig()
            {
                RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName("us-east-1"),
                UseHttp = true
            };

            client = new AmazonDynamoDBClient(config);
            tableName = args[0];
            taskDelay = int.Parse(args[1]);

            int numTasks = 1;
            if (args.Length > 2)
            {
                numTasks = int.Parse(args[2]);
            }

            if (args.Length > 3)
            {
                requestBatching = int.Parse(args[3]);
            }

            List<Task> loops = new List<Task>();
            for (int i = 0; i < numTasks; i++)
            {
                loops.Add(RunLoop());
            }

            Console.WriteLine("Running.  Press Q to quit.");
            int sleeps = 0;
            while (!cts.IsCancellationRequested)
            {
                Thread.Sleep(100);
                sleeps++;
                if (sleeps > 9)
                {
                    sleeps = 0;
                    Console.WriteLine("{0} items submitted.", RequestCount);
                }

                if (Console.KeyAvailable && Console.ReadKey(true).Key == ConsoleKey.Q)
                {
                    cts.Cancel();
                }
            }

            Console.WriteLine("Waiting for tasks to end.");
            Task.WaitAll(loops.ToArray());
        }

        public static async Task RunLoop()
        {
            var ct = cts.Token;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    List<Task<UpdateItemResponse>> updateCalls = new List<Task<UpdateItemResponse>>();
                    for (int i = 0; i < requestBatching; i++)
                    {
                        string key = Guid.NewGuid().ToString();
                        long dataValue = Interlocked.Increment(ref RequestCount);

                        var keyAttr = new Dictionary<string, AttributeValue>() { { "key", new AttributeValue(key) } };
                        var request = new UpdateItemRequest() { TableName = tableName, Key = keyAttr };
                        request.UpdateExpression = "ADD #datavalue :value";
                        var valueSet = new List<string>() { dataValue.ToString() };
                        request.ExpressionAttributeValues = new Dictionary<string, AttributeValue>() { { ":value", new AttributeValue() { NS = valueSet } } };
                        request.ExpressionAttributeNames = new Dictionary<string, string>() { { "#datavalue", "datavalue" } };

                        updateCalls.Add(client.UpdateItemAsync(request, ct));
                    }

                    var results = await Task.WhenAll(updateCalls);
                    foreach (var result in results)
                    {
                        if ((int)result.HttpStatusCode >= 400)
                        {
                            Console.WriteLine("Error from dynamo {0}", result.HttpStatusCode);
                        }
                    }
                }
                catch (TaskCanceledException)
                {
                    // Eat this exception
                }
                catch (Exception ex)
                {
                    Console.WriteLine("!! Exception: {0}", ex);
                    if (ct.IsCancellationRequested)
                        return;
                    await Task.Delay(Math.Max(50, taskDelay * 4));
                }
                if (ct.IsCancellationRequested)
                    return;
                if (taskDelay > 0)
                    await Task.Delay(taskDelay);
            }
        }

    }
}
