using System.IO;
using System.Threading.Tasks;

namespace MapReduce
{
    public class StreamExtensions
    {
        public static async Task RemoveNewLineFromEndOfFileAsync(StreamWriter streamWriter)
        {
            await streamWriter.FlushAsync();
            if (streamWriter.BaseStream.Length > 2)
            {
                streamWriter.BaseStream.Seek(-2, SeekOrigin.End);
                streamWriter.BaseStream.SetLength(streamWriter.BaseStream.Position);
            }
        }
    }
}
