using System.IO;
using System.Threading.Tasks;

namespace MapReduce
{
    public static class FileExtensions
    {
        public static async Task<int> GetCountOfLinesInFile(string fileName)
        {
            int countOfLines = 0;
            
            using (StreamReader reader = new StreamReader(fileName))
            {
                while (!reader.EndOfStream)
                {
                    reader.ReadLine();
                    countOfLines++;
                }
            }

            return countOfLines;
        }
    }
}