using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MapReduce;

namespace Laba_1
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var inputFileName = "input2.txt";
            await MapReduceExtensions.DoMapReduce(inputFileName);
        }
    }
}
