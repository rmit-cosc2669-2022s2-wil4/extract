using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Extract;

internal class Program
{
    static void Main(string[] args)
    {
        if (args.Length < 1)
            return;

        if (string.Equals(args[0], "Single", StringComparison.OrdinalIgnoreCase))
            SingleExtract.Run(args.Skip(1).ToArray());
        else if (string.Equals(args[0], "Combined", StringComparison.OrdinalIgnoreCase))
            CombinedExtract.Run(args.Skip(1).ToArray());
    }
}