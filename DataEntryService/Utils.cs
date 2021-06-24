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

    }
}
