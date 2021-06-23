using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace DataEntryService
{
    class Program
    {
        private static string connectionString = "Endpoint=sb://learnquque.servicebus.windows.net/;SharedAccessKeyName=myaccess;SharedAccessKey=agyIY+H3HnSVv2xgmKVgtTXTy4KEL1rKF8bUDoiDqQE=";
        private static string queueName = "filequeue";

        private static ServiceBusClient _client;

        private static ServiceBusSender _senderBus;

        private const string WorkFolderPath = @"C:\LearnPdf";
        private const int MessageMaxSize = 1024 * 256;
        private static FileSystemWatcher watcher = new FileSystemWatcher(WorkFolderPath);
        private static string _clientId;

        static async Task Main(string[] args)
        {
            // Create the clients that we'll use for sending and processing messages.
            _client = new ServiceBusClient(connectionString);
            _senderBus = _client.CreateSender(queueName);

            _clientId = Guid.NewGuid().ToString();

            ConfigureFileSystemWatcher();

            Console.WriteLine("Press any key to end the application");
            Console.ReadKey();
        }

        private static void ConfigureFileSystemWatcher()
        {

            if (!Directory.Exists(WorkFolderPath))
            {
                Directory.CreateDirectory(WorkFolderPath);
            }

            watcher.NotifyFilter = NotifyFilters.Attributes
                                 | NotifyFilters.CreationTime
                                 | NotifyFilters.DirectoryName
                                 | NotifyFilters.FileName
                                 | NotifyFilters.LastAccess
                                 | NotifyFilters.LastWrite
                                 | NotifyFilters.Security
                                 | NotifyFilters.Size;

            watcher.Created += HandleFileChanges;
            watcher.Renamed += HandleFileChanges;

            watcher.Filter = "*.pdf";
            watcher.IncludeSubdirectories = false;
            watcher.EnableRaisingEvents = true;
        }

        private static void HandleFileChanges(object sender, FileSystemEventArgs e)
        {
            ProcessMessage(e.FullPath, e.Name);
            
        }

        private static void ProcessMessage(string fullPath, string fileName)
        {
            var fileBytes = Utils.GetBinaryFile(fullPath);

            try
            {
                //check size of message 
                if (fileBytes.Length > MessageMaxSize)
                {
                    SendFileByChunks(fullPath, fileBytes,  fileName);
                }
                else
                {
                    SendMessage( _clientId, fileBytes, fullPath, fileName);
                }

                Console.WriteLine($"A batch of {fullPath} messages has been published to the queue.");

            }
            finally
            {
                _senderBus.DisposeAsync().GetAwaiter().GetResult();
                _client.DisposeAsync().GetAwaiter().GetResult();
            }
        }

        private static void SendFileByChunks(string fullPath, byte[] fileBytes, string fileName)
        {

            int chunksCount = (fileBytes.Length % MessageMaxSize) == 0
                ? (fileBytes.Length / MessageMaxSize)
                : (fileBytes.Length / MessageMaxSize) + 1;

            for (int i = 0; i < chunksCount; i++)
            {
                
                int beginPosition = i * MessageMaxSize == 0 ? 0 : i * MessageMaxSize - 1024;
                int lengthOfArray = MessageMaxSize - 1024;

                int remainingBytes = fileBytes.Length - beginPosition;

                if (remainingBytes < MessageMaxSize) lengthOfArray = remainingBytes;

                var chunkArray = Utils.SubArray(fileBytes, beginPosition, lengthOfArray);
                
                SendMessage( _clientId, chunkArray, fullPath, fileName);

            }
        }

        private static void SendMessage( string clientId, byte[] fileBytes, string fullPath, string fileName)
        {
            using ServiceBusMessageBatch messageBatch = _senderBus.CreateMessageBatchAsync().GetAwaiter().GetResult();

            var message = new ServiceBusMessage(new BinaryData(fileBytes));
            message.ApplicationProperties.Add("ClientId", clientId);
            message.ApplicationProperties.Add("FileName", fileName);

            if (!messageBatch.TryAddMessage(message))
            {
                throw new Exception($"The message {fullPath} is too large to fit in the batch.");
            }

            _senderBus.SendMessagesAsync(messageBatch).GetAwaiter().GetResult();

        }

    }
}
