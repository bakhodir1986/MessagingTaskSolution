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
        private const int MessageMaxSize = 1024 * 255;
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

            await _senderBus.DisposeAsync();
            await _client.DisposeAsync();
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

        private static async void HandleFileChanges(object sender, FileSystemEventArgs e)
        {
            await ProcessMessage(e.FullPath, e.Name);
        }

        private static async Task ProcessMessage(string fullPath, string fileName)
        {
            await SendFile(fullPath, fileName);

            Console.WriteLine($"A batch of {fullPath} messages has been published to the queue.");
        }

        private static async Task SendFile(string fullPath , string filename)
        {
            await using FileStream file = new FileStream(fullPath, FileMode.Open, FileAccess.Read);

            long fileLength = file.Length;
            bool isFileMoreThanMax = fileLength > MessageMaxSize;
            long lengthOfStartingArray = isFileMoreThanMax ? MessageMaxSize : fileLength;
            var bytes = new byte[lengthOfStartingArray];

            int offsetIndex = 0;

            while (file.Read(bytes, 0, MessageMaxSize) != 0)
            {
                await SendMessage(_clientId, bytes, filename, offsetIndex * MessageMaxSize);

                if (isFileMoreThanMax)
                {
                    offsetIndex++;

                    long remaining = fileLength - offsetIndex * MessageMaxSize;
                    bool isRemainingMoreThanMax = remaining > MessageMaxSize;

                    bytes = new byte[isRemainingMoreThanMax ? MessageMaxSize : remaining]; //clear buffer
                }
            }
        }
        

        private static async Task SendMessage( string clientId, byte[] fileBytes, string fileName, int offset)
        {
            var message = new ServiceBusMessage(new BinaryData(fileBytes));
            message.ApplicationProperties.Add("ClientId", clientId);
            message.ApplicationProperties.Add("FileName", fileName);
            message.ApplicationProperties.Add("Offset", offset);

            await _senderBus.SendMessageAsync(message);
        }

    }
}
