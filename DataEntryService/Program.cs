using System;
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

        static async Task Main(string[] args)
        {
            // Create the clients that we'll use for sending and processing messages.
            _client = new ServiceBusClient(connectionString);
            _senderBus = _client.CreateSender(queueName);

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
            ProcessMessage(e.FullPath);
        }

        private static void ProcessMessage(string fullPath)
        {
            var fileBytes = Utils.GetBinaryFile(fullPath);
            using ServiceBusMessageBatch messageBatch = _senderBus.CreateMessageBatchAsync().GetAwaiter().GetResult();

            //check size of message 
            if (fileBytes.Length > MessageMaxSize)
            {
                SendFileByChunks(fullPath, fileBytes, messageBatch);
            }
            else
            {
                SendMessage(messageBatch, null, fileBytes, fullPath);
            }

            try
            {
                _senderBus.SendMessagesAsync(messageBatch).GetAwaiter().GetResult();
                Console.WriteLine($"A batch of {fullPath} messages has been published to the queue.");
            }
            finally
            {
                _senderBus.DisposeAsync().GetAwaiter().GetResult();
                _senderBus.DisposeAsync().GetAwaiter().GetResult();
            }
        }

        private static void SendFileByChunks(string fullPath, byte[] fileBytes, ServiceBusMessageBatch messageBatch)
        {
            string correlationId = Guid.NewGuid().ToString();

            int chunksCount = (fileBytes.Length % MessageMaxSize) == 0
                ? (fileBytes.Length / MessageMaxSize)
                : (fileBytes.Length / MessageMaxSize) + 1;

            for (int i = 0; i < chunksCount; i++)
            {
                int beginPosition = i * MessageMaxSize;
                int lengthOfArray = MessageMaxSize - 1;

                int remainingBytes = fileBytes.Length - beginPosition;

                if (remainingBytes < MessageMaxSize) lengthOfArray = remainingBytes;

                var chunkArray = Utils.SubArray(fileBytes, beginPosition, lengthOfArray);
                SendMessage(messageBatch, correlationId, chunkArray, fullPath);
            }
        }

        private static void SendMessage(ServiceBusMessageBatch messageBatch, string correlationId, byte[] fileBytes, string fullPath)
        {
            var message = new ServiceBusMessage(new BinaryData(fileBytes)) {CorrelationId = correlationId };

            if (!messageBatch.TryAddMessage(message))
            {
                throw new Exception($"The message {fullPath} is too large to fit in the batch.");
            }
        }

    }
}
