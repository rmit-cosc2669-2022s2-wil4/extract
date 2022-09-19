using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Extract;

public class SingleExtract
{
    public record RowRec(int StationNo, DateOnly From, DateOnly To, float Val)
    {
        public static RowRec Parse(int stationNo, string line)
        {
            var vals = line.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return new RowRec(stationNo, DateOnly.ParseExact(vals[0], "yyyyMMdd"), DateOnly.ParseExact(vals[1], "yyyyMMdd"), float.Parse(vals[2]));
        }
    }

    public static void Run(string[] args)
    {
        if (args.Length < 1)
            return;

        ISet<int>? stations = null;

        if (args.Length > 1)
            stations = new HashSet<int>(args[1].Split(',').Select(int.Parse));

        var outputs = new Dictionary<string, List<RowRec>>();

        byte[] buffer = new byte[10_000_000];
        var ascii = new ASCIIEncoding();

        foreach (var zfn in Directory.EnumerateFiles(args[0]).Where(x => x.EndsWith(".Z", StringComparison.OrdinalIgnoreCase)))
        {
            var p = zfn.Split('.');

            var fileType = Path.GetFileName(p[0]);
            int stationNo = int.Parse(p[1]);
            string dataType = p[2];

            Debug.WriteLine(zfn);
            if (!(stations?.Contains(stationNo) ?? true))
                continue;


            using Stream zfs = new LzwInputStream(File.OpenRead(zfn));

            int read;
            if ((read = zfs.Read(buffer, 0, buffer.Length)) >= buffer.Length)
                Debug.WriteLine("Doh");

            var recs = ascii.GetString(buffer, 0, read)
                .Split("\r\n")
                .Skip(1)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => RowRec.Parse(stationNo, x))
                .ToList();

            string outKey = fileType + "." + dataType;

            if (outputs.ContainsKey(outKey))
                outputs[outKey].AddRange(recs);
            else
                outputs[outKey] = recs;
        }

        foreach(var (t, d) in outputs)
        {
            Debug.WriteLine($"{t}, {d.Count}");
            File.WriteAllLines(t + ".csv", d.Select(x => $"{x.StationNo},{x.From:yyyy-MM-dd},{x.To:yyyy-MM-dd},{x.Val}").ToArray());
        }
    }
}