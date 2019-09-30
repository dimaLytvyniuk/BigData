using System;
using System.IO;
using System.Text;
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
                    
                    await StreamExtensions.RemoveNewLineFromEndOfFileAsync(writerFirst);
                    await StreamExtensions.RemoveNewLineFromEndOfFileAsync(writerSecond);
                }

                using (var writer = new StreamWriter(sortedFile, false))
                using (var readerFirst = new StreamReader(tmpFile1))
                using (var readerSecond = new StreamReader(tmpFile2))
                {
                    var isReadFromFirst = true;
                    var isReadFromSecond = true;
                    var fromFirstLine = "";
                    var fromSecondLine = "";
                    
                    while (!readerFirst.EndOfStream && !readerSecond.EndOfStream)
                    {
                        var writedFromFirstLines = 0;
                        var writedFromSecondLines = 0;
                        isReadFromFirst = true;
                        isReadFromSecond = true;
                        
                        while (
                            writedFromFirstLines < seriesSize && 
                            writedFromSecondLines < seriesSize && 
                            (!readerFirst.EndOfStream || !isReadFromFirst) &&
                            (!readerSecond.EndOfStream || !isReadFromSecond))
                        {
                            if (isReadFromFirst)
                            {
                                fromFirstLine = await readerFirst.ReadLineAsync();
                                isReadFromFirst = false;
                            }

                            if (isReadFromSecond)
                            {
                                fromSecondLine = await readerSecond.ReadLineAsync();
                                isReadFromSecond = false;
                            }
                            
                            if (CompareMapReduceStr(fromFirstLine, fromSecondLine) < 1)
                            {
                                await writer.WriteLineAsync(fromFirstLine);
                                writedFromFirstLines++;
                                isReadFromFirst = true;
                            }
                            else
                            {
                                await writer.WriteLineAsync(fromSecondLine);
                                writedFromSecondLines++;
                                isReadFromSecond = true;
                            }
                        }

                        if (!isReadFromFirst && writedFromFirstLines < seriesSize)
                        {
                            await writer.WriteLineAsync(fromFirstLine);
                            writedFromFirstLines++;
                            isReadFromFirst = true;
                        }
                        
                        while (writedFromFirstLines < seriesSize && !readerFirst.EndOfStream)
                        {
                            fromFirstLine = await readerFirst.ReadLineAsync();
                            await writer.WriteLineAsync(fromFirstLine);
                            writedFromFirstLines++;
                            isReadFromFirst = true;
                        }

                        if (!isReadFromSecond && writedFromSecondLines < seriesSize)
                        {
                            await writer.WriteLineAsync(fromSecondLine);
                            writedFromSecondLines++;
                            isReadFromSecond = true;
                        }
                        
                        while (writedFromSecondLines < seriesSize && !readerSecond.EndOfStream)
                        {
                            fromSecondLine = await readerSecond.ReadLineAsync();
                            await writer.WriteLineAsync(fromSecondLine);
                            writedFromSecondLines++;
                            isReadFromSecond = true;
                        }
                    }

                    if (!isReadFromFirst)
                    {
                        await writer.WriteLineAsync(fromFirstLine);
                    }

                    if (!isReadFromSecond)
                    {
                        await writer.WriteLineAsync(fromSecondLine);
                    }
                    
                    while (!readerFirst.EndOfStream)
                    {
                        fromFirstLine = await readerFirst.ReadLineAsync();
                        await writer.WriteLineAsync(fromFirstLine);
                    }
                    
                    while (!readerSecond.EndOfStream)
                    {
                        fromSecondLine = await readerSecond.ReadLineAsync();
                        await writer.WriteLineAsync(fromSecondLine);
                    }
                    
                    await StreamExtensions.RemoveNewLineFromEndOfFileAsync(writer);
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
