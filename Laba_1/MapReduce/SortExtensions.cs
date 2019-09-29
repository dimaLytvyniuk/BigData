using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MapReduce
{
    public static class SortExtensions
    {
        public static async Task SortMapReduceFileAsync(string sortedFile)
        {
            var tmpFile1 = $"{Guid.NewGuid().ToString()}.txt";
            var tmpFile2 = $"{Guid.NewGuid().ToString()}.txt";
            var seriesSize = 1;
            var fileCountOfLines = await FileExtensions.GetCountOfLinesInFile(sortedFile);

            while (seriesSize < fileCountOfLines)
            {
                using (var reader = new StreamReader(sortedFile))
                using (var writerFirst = new StreamWriter(tmpFile1, false))
                using (var writerSecond = new StreamWriter(tmpFile2, false))
                {
                    while (!reader.EndOfStream)
                    {
                        for (int i = 0; i < seriesSize && !reader.EndOfStream; i++)
                        {
                            var line = await reader.ReadLineAsync();
                            await writerFirst.WriteLineAsync(line);   
                        }
                        
                        for (int i = 0; i < seriesSize && !reader.EndOfStream; i++)
                        {
                            var line = await reader.ReadLineAsync();
                            await writerSecond.WriteLineAsync(line);   
                        }
                    }
                }

                using (var writer = new StreamWriter(sortedFile, false))
                using (var readerFirst = new StreamReader(tmpFile1))
                using (var readerSecond = new StreamReader(tmpFile2))
                {
                    var fromFirstLine = await readerFirst.ReadLineAsync();
                    var fromSecondLine = await readerSecond.ReadLineAsync();
                    
                    while (!readerFirst.EndOfStream && !readerSecond.EndOfStream)
                    {
                        var writedFromFirstLines = 0;
                        var writedFromSecondLines = 0;
                        
                        while (
                            writedFromFirstLines < seriesSize && 
                            writedFromSecondLines < seriesSize && 
                            !readerFirst.EndOfStream && 
                            !readerSecond.EndOfStream)
                        {
                            if (CompareMapReduceStr(fromFirstLine, fromSecondLine) < 1)
                            {
                                await writer.WriteLineAsync(fromFirstLine);
                                writedFromFirstLines++;
                                fromFirstLine = await readerFirst.ReadLineAsync();
                            }
                            else
                            {
                                await writer.WriteLineAsync(fromSecondLine);
                                writedFromSecondLines++;
                                fromSecondLine = await readerSecond.ReadLineAsync();
                            }
                        }

                        while (writedFromFirstLines < seriesSize && !readerFirst.EndOfStream)
                        {
                            await writer.WriteLineAsync(fromFirstLine);
                            writedFromFirstLines++;
                            fromFirstLine = await readerFirst.ReadLineAsync();
                        }
                        
                        while (writedFromSecondLines < seriesSize && !readerSecond.EndOfStream)
                        {
                            await writer.WriteLineAsync(fromSecondLine);
                            writedFromSecondLines++;
                            fromSecondLine = await readerSecond.ReadLineAsync();
                        }
                    }

                    while (!readerFirst.EndOfStream)
                    {
                        await writer.WriteLineAsync(fromFirstLine);
                        fromFirstLine = await readerFirst.ReadLineAsync();
                    }
                    
                    while (!readerSecond.EndOfStream)
                    {
                        await writer.WriteLineAsync(fromSecondLine);
                        fromSecondLine = await readerSecond.ReadLineAsync();
                    }
                }

                seriesSize *= 2;
            }

            File.Delete(tmpFile1);
            File.Delete(tmpFile2);
        }

        private static int CompareMapReduceStr(string firstStr, string secondStr)
        {
            firstStr = firstStr.Split(",")[0];
            secondStr = secondStr.Split(",")[0];

            return firstStr.CompareTo(secondStr);
        }
    }
}
