using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MapReduce
{
    public static class MapReduceExtensions
    {
        public static async Task DoMapReduce(string inputFileName)
        {
            await MapAsync(inputFileName);
            await ShuffleAsync();
            //await ReduceAsync();
        }
        
        public static async Task MapAsync(string inputFileName, string outputFile1 = "file1.txt", string outputFile2 = "file2.txt")
        {
            using (StreamReader reader = new StreamReader(inputFileName))
            using (StreamWriter writerFirst = new StreamWriter(outputFile1, false))
            using (StreamWriter writerSecond = new StreamWriter(outputFile2, false))
            {
                StreamWriter currentWriter = writerFirst;
                StreamWriter anotherWriter = writerSecond;
                StreamWriter tmpWriter;
                
                while (!reader.EndOfStream)
                {
                    var line = await reader.ReadLineAsync();
                    if (line == null)
                    {
                        continue;    
                    }
                    
                    line = Regex.Replace(line, @"[\p{P}\n]", "");
                    if (String.IsNullOrEmpty(line))
                    {
                        continue;
                    }
                    
                    var words = line.Split(" ");

                    foreach (var word in words)
                    {
                        if (String.IsNullOrEmpty(word))
                        {
                            continue;
                        }
                        
                        await currentWriter.WriteLineAsync($"{word},1");
                        
                        tmpWriter = currentWriter;
                        currentWriter = anotherWriter;
                        anotherWriter = tmpWriter;
                    }
                }
            }
        }

        public static async Task ShuffleAsync(
            string input1File = "file1.txt",
            string input2File = "file2.txt",
            string output1File = "shuffle1.txt",
            string output2File = "shuffle2.txt")
        {
            await PrepareToSortFiles(input1File, input2File, output1File, output2File);
            
            var sortInputFilesTasks = new Task[]
            {
                SortExtensions.SortMapReduceFileAsync(output1File),
                SortExtensions.SortMapReduceFileAsync(output2File),
            };

            await Task.WhenAll(sortInputFilesTasks);
        }

        public static async Task ReduceAsync(
            string input1File = "shuffle1.txt",
            string input2File = "shuffle2.txt",
            string outputFile = "reduce.txt")
        {
            var tmpFirstFile = $"{Guid.NewGuid().ToString()}.txt";
            var tmpSecondFile = $"{Guid.NewGuid().ToString()}.txt";

            var doReduceTasks = new Task[]
            {
                DoReduceTaskAsync(input1File, tmpFirstFile),
                DoReduceTaskAsync(input2File, tmpSecondFile)
            };
            await Task.WhenAll(doReduceTasks);

            using (var writer = new StreamWriter(outputFile))
            using (var readerFirst = new StreamReader(tmpFirstFile))
            using (var readerSecond = new StreamReader(tmpSecondFile))
            {
                while (!readerFirst.EndOfStream)
                {
                    var line = await readerFirst.ReadLineAsync();
                    await writer.WriteLineAsync(line);
                }

                while (!readerSecond.EndOfStream)
                {
                    var line = await readerSecond.ReadLineAsync();
                    await writer.WriteLineAsync(line);
                }
            }

            File.Delete(tmpFirstFile);
            File.Delete(tmpSecondFile);
        }

        private static async Task DoReduceTaskAsync(string inputFile, string outputFile)
        {
            using (var reader = new StreamReader(inputFile))
            using (var writer = new StreamWriter(outputFile))
            {
                var сurrentline = await reader.ReadLineAsync();
                var currentSplitted = сurrentline.Split(",");
                var currentWord = currentSplitted[0];
                var currentCountOfWord = Int32.Parse(currentSplitted[1]);
                
                while (!reader.EndOfStream)
                {
                    var nextLine = await reader.ReadLineAsync();
                    var nextSplitted = nextLine.Split(",");
                    var nextWord = nextSplitted[0];

                    if (nextWord == currentWord)
                    {
                        currentCountOfWord += Int32.Parse(nextSplitted[1]);
                    }
                    else
                    {
                        await writer.WriteLineAsync($"{currentWord},{currentCountOfWord}");
                        currentWord = nextWord;
                        currentCountOfWord = Int32.Parse(nextSplitted[1]);
                    }
                }
                await writer.WriteLineAsync($"{currentWord},{currentCountOfWord}");
            }
        }
        
        private static async Task PrepareToSortFiles(
            string input1File = "file1.txt",
            string input2File = "file2.txt",
            string output1File = "shuffle1.txt",
            string output2File = "shuffle2.txt")
        {
            using (var readerFirst = new StreamReader(input1File))
            using (var readerSecond = new StreamReader(input2File))
            using (var writerFirst = new StreamWriter(output1File))
            using (var writerSecond = new StreamWriter(output2File))
            {
                while (!readerFirst.EndOfStream)
                {
                    var line = await readerFirst.ReadLineAsync();
                    if (Char.ToLower(line[0]) < 'n')
                    {
                        await writerFirst.WriteLineAsync(line);
                    }
                    else
                    {
                        await writerSecond.WriteLineAsync(line);
                    }
                }
                
                while (!readerSecond.EndOfStream)
                {
                    var line = await readerSecond.ReadLineAsync();
                    if (Char.ToLower(line[0]) < 'n')
                    {
                        await writerFirst.WriteLineAsync(line);
                    }
                    else
                    {
                        await writerSecond.WriteLineAsync(line);
                    }
                }
            }
        }
    }
}
