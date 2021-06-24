using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CentralServer
{
    public class Utils
    {
        public static void AppendAllBytes(string path, byte[] bytes)
        {
            using var stream = new FileStream(path, FileMode.Append);
            stream.Write(bytes, 0, bytes.Length);
        }

        public static void InsertIntoFileByChunks(string path, byte[] bytes, int offset)
        {
            using var stream = new FileStream(path, FileMode.Append) {Position = offset};
            stream.Write(bytes, 0, bytes.Length);
        }
    }
}
