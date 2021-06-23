using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace CentralServer
{
    public class TransferFile
    {
        public string Path { get; set; }

        public string Name { get; set; }

        public override string ToString()
        {
            return JsonSerializer.Serialize(this);
        }
    }
}
