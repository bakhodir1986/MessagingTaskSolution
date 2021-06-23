using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DataEntryService
{
    public static class Utils
    {
        public static byte[] SubArray(byte[] data, int index, int length)
        {
            byte[] result = new byte[length];
            Array.Copy(data, index, result, 0, length);
            return result;
        }

        public static byte[] GetBinaryFile(string filename)
        {
            using FileStream file = new FileStream(filename, FileMode.Open, FileAccess.Read);
            var bytes = new byte[file.Length];
            file.Read(bytes, 0, (int)file.Length);
            return bytes;
        }
    }
}
