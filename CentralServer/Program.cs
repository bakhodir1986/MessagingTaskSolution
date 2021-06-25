using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace CentralServer
{
    class Program
    {
        private static string connectionString = "Endpoint=sb://learnquque.servicebus.windows.net/;SharedAccessKeyName=myaccess;SharedAccessKey=agyIY+H3HnSVv2xgmKVgtTXTy4KEL1rKF8bUDoiDqQE=";
        private static string queueName = "filequeue";
        private const string WorkFolder = @"C:\LearnPdf\Server";

        static async Task Main(string[] args)
        {
            var client = new ServiceBusClient(connectionString);

            var processor = client.CreateProcessor(queueName);

            try
            {
                // add handler to process messages
                processor.ProcessMessageAsync += MessageHandler;

                // add handler to process any errors
                processor.ProcessErrorAsync += ErrorHandler;

                // start processing 
                await processor.StartProcessingAsync();

                Console.WriteLine("Wait for a minute and then press any key to end the processing");
                Console.ReadKey();

                // stop processing 
                Console.WriteLine("\nStopping the receiver...");
                await processor.StopProcessingAsync();
                Console.WriteLine("Stopped receiving messages");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await processor.DisposeAsync();
                await client.DisposeAsync();
            }
        }

        private static Task ErrorHandler(ProcessErrorEventArgs arg)
        {
            Console.WriteLine(arg.Exception.ToString());

            return Task.CompletedTask;
        }

        private static async Task MessageHandler(ProcessMessageEventArgs arg)
        {
            string fileName = arg.Message.ApplicationProperties["FileName"].ToString();
            string clientId = arg.Message.ApplicationProperties["ClientId"].ToString();
            int offset = (int)arg.Message.ApplicationProperties["Offset"];

            if (string.IsNullOrEmpty(clientId) || string.IsNullOrEmpty(fileName))
            {
                return;
            }

            string saveFolder = Path.Combine(WorkFolder, clientId);

            if (!Directory.Exists(saveFolder))
            {
                Directory.CreateDirectory(saveFolder);
            }

            Utils.InsertIntoFileByChunks(Path.Combine(saveFolder, fileName)
                , arg.Message.Body.ToArray(), offset);

        }
    }
}
