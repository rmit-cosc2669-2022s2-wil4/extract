using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace Extract;

internal class CombinedExtract
{
    public static void Run(string[] args) => new CombinedExtract().Extract(args);

    public record RowRec(string Metric, int StationNo, DateOnly From, DateOnly To, float Val)
    {
        public static RowRec Parse(string metric, int stationNo, string line)
        {
            var vals = line.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return new RowRec(metric, stationNo, DateOnly.ParseExact(vals[0], "yyyyMMdd"), DateOnly.ParseExact(vals[1], "yyyyMMdd"), float.Parse(vals[2]));
        }
    }

    readonly Encoding _ascii = new ASCIIEncoding();

    public void Extract(string[] args)
    {
        if (args.Length < 1)
            return;

        var folders = args[0].Split(',');

        ISet<int>? stations = null;
        if (args.Length > 1)
            stations = new HashSet<int>(args[1].Split(',').Select(int.Parse));

        var output = new Dictionary<string, List<RowRec>>();
        byte[] buffer = new byte[10_000_000];

        foreach (var folder in folders)
        {
            var dir = $".\\{folder}\\Data";

            foreach (var zfn in Directory.EnumerateFiles(dir).Where(x => x.EndsWith(".Z", StringComparison.OrdinalIgnoreCase)))
            {
                var p = zfn.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                var metric = Path.GetFileName(p[0]);
                int stationNo = int.Parse(p[1]);
                string dataFreq = p[2];

                Debug.WriteLine(zfn);
                if (!(stations?.Contains(stationNo) ?? true))
                    continue;

                using Stream zfs = new LzwInputStream(File.OpenRead(zfn));

                int read;
                if ((read = zfs.Read(buffer, 0, buffer.Length)) >= buffer.Length)
                    Debug.WriteLine("Doh");

                var recs = _ascii.GetString(buffer, 0, read)
                    .Split("\r\n")
                    .Skip(1)
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Select(x => RowRec.Parse(metric, stationNo, x))
                    .ToList();

                
                if (output.ContainsKey(dataFreq))
                    output[dataFreq].AddRange(recs);
                else
                    output[dataFreq] = recs;
            }
        }

        foreach(var (df, rows) in output)
        {
            Debug.WriteLine($"{df}, {rows.Count}");

            using var f = new StreamWriter(df + ".csv");
            
            var metrics = rows.Select(x => x.Metric).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(x => x).ToList();

            f.WriteLine($"Date,StationNo,{string.Join(',', metrics)}");

            foreach (var row in rows.GroupBy(x => (x.From, x.StationNo))
                .OrderBy(x => x.Key.From).ThenBy(x => x.Key.StationNo))
            {
                var cols = new List<string> { row.Key.From.ToString("yyyy-MM-dd"), row.Key.StationNo.ToString() };
                cols.AddRange(metrics.Select(m => row.FirstOrDefault(x => string.Equals(x.Metric, m, StringComparison.OrdinalIgnoreCase))?.Val.ToString() ?? string.Empty));

                f.WriteLine(string.Join(',', cols));
            }
        }
    }
}