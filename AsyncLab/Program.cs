using System.Diagnostics;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Linq;

// =================== Configuração ===================
const int PBKDF2_ITERATIONS = 50_000;
const int HASH_BYTES = 32;
const string CSV_URL = "https://www.gov.br/receitafederal/dados/municipios.csv";
const string OUT_DIR_NAME = "mun_hash_por_uf";

string FormatTempo(long ms)
{
    var ts = TimeSpan.FromMilliseconds(ms);
    return $"{ts.Minutes}m {ts.Seconds}s {ts.Milliseconds}ms";
}

var sw = Stopwatch.StartNew();
string baseDir = Directory.GetCurrentDirectory();
string tempCsvPath = Path.Combine(baseDir, "municipios.csv");
string outRoot = Path.Combine(baseDir, OUT_DIR_NAME);

Console.WriteLine("=== Verificando arquivo local ===");
bool precisaBaixar = true;
string csvData = "";

if (File.Exists(tempCsvPath))
{
    Console.WriteLine("Arquivo local já existe. Comparando com versão online...");
    using (var client = new HttpClient())
    {
        var novoCsv = await client.GetStringAsync(CSV_URL);
        var antigoCsv = await File.ReadAllTextAsync(tempCsvPath, Encoding.UTF8);

        if (novoCsv == antigoCsv)
        {
            Console.WriteLine("O arquivo local já está atualizado.");
            csvData = antigoCsv;
            precisaBaixar = false;
        }
        else
        {
            Console.WriteLine("Diferença encontrada! Salvando diferenças em municipios_diff.csv");
            var diffPath = Path.Combine(baseDir, "municipios_diff.csv");
            var diff = novoCsv.Split('\n').Except(antigoCsv.Split('\n'));
            await File.WriteAllLinesAsync(diffPath, diff, Encoding.UTF8);

            csvData = novoCsv;
            await File.WriteAllTextAsync(tempCsvPath, novoCsv, Encoding.UTF8);
        }
    }
}
if (precisaBaixar)
{
    Console.WriteLine("Baixando CSV de municípios (Receita Federal) ...");
    using (var client = new HttpClient())
    {
        csvData = await client.GetStringAsync(CSV_URL);
        await File.WriteAllTextAsync(tempCsvPath, csvData, Encoding.UTF8);
    }
}

Console.WriteLine("Lendo e parseando o CSV ...");
var linhas = csvData.Split('\n');
if (linhas.Length == 0)
{
    Console.WriteLine("Arquivo CSV vazio.");
    return;
}

int startIndex = 0;
if (linhas[0].IndexOf("IBGE", StringComparison.OrdinalIgnoreCase) >= 0 ||
    linhas[0].IndexOf("UF", StringComparison.OrdinalIgnoreCase) >= 0)
{
    startIndex = 1; // pula cabeçalho
}

var municipios = new List<Municipio>(linhas.Length - startIndex);
for (int i = startIndex; i < linhas.Length; i++)
{
    var linha = (linhas[i] ?? "").Trim();
    if (string.IsNullOrWhiteSpace(linha)) continue;

    var parts = linha.Split(';');
    if (parts.Length < 5) continue;

    municipios.Add(new Municipio
    {
        Tom = Util.San(parts[0]),
        Ibge = Util.San(parts[1]),
        NomeTom = Util.San(parts[2]),
        NomeIbge = Util.San(parts[3]),
        Uf = Util.San(parts[4]).ToUpperInvariant()
    });
}

Console.WriteLine($"Registros lidos: {municipios.Count}");

// Grupo por UF
var porUf = municipios.GroupBy(m => m.Uf)
                      .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.OrdinalIgnoreCase);

var ufsOrdenadas = porUf.Keys
    .Where(uf => !string.Equals(uf, "EX", StringComparison.OrdinalIgnoreCase))
    .OrderBy(uf => uf, StringComparer.OrdinalIgnoreCase)
    .ToList();

// Saída
Directory.CreateDirectory(outRoot);
Console.WriteLine("Calculando hash por município e gerando arquivos por UF ...");

var tasks = new List<Task>();

foreach (var uf in ufsOrdenadas)
{
    tasks.Add(Task.Run(async () =>
    {
        var listaUf = porUf[uf];
        listaUf.Sort((a, b) => string.Compare(a.NomePreferido, b.NomePreferido, StringComparison.OrdinalIgnoreCase));

        Console.WriteLine($"Processando UF: {uf} ({listaUf.Count} municípios)");
        var swUf = Stopwatch.StartNew();
        string outPath = Path.Combine(outRoot, $"municipios_hash_{uf}.csv");
        using (var fs = new FileStream(outPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var swOut = new StreamWriter(fs, new UTF8Encoding(false)))
        {
            swOut.WriteLine("TOM;IBGE;NomeTOM;NomeIBGE;UF;Hash");

            var listaJson = new List<object>();
            string binPath = Path.Combine(outRoot, $"municipios_{uf}.bin");
            using var binWriter = new BinaryWriter(File.Open(binPath, FileMode.Create));

            foreach (var m in listaUf)
            {
                string password = m.ToConcatenatedString();
                byte[] salt = Util.BuildSalt(m.Ibge);
                string hashHex = Util.DeriveHashHex(password, salt, PBKDF2_ITERATIONS, HASH_BYTES);

                swOut.WriteLine($"{m.Tom};{m.Ibge};{m.NomeTom};{m.NomeIbge};{m.Uf};{hashHex}");

                listaJson.Add(new { m.Tom, m.Ibge, m.NomeTom, m.NomeIbge, m.Uf, Hash = hashHex });

                // Salva em binário (TOM, IBGE, Nome, UF)
                binWriter.Write(m.Tom);
                binWriter.Write(m.Ibge);
                binWriter.Write(m.NomePreferido);
                binWriter.Write(m.Uf);
            }

            string jsonPath = Path.Combine(outRoot, $"municipios_hash_{uf}.json");
            var json = JsonSerializer.Serialize(listaJson, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(jsonPath, json, Encoding.UTF8);

            swUf.Stop();
            Console.WriteLine($"UF {uf} concluída. Arquivos gerados: CSV, JSON e BIN.");
        }
    }));
}

await Task.WhenAll(tasks);
sw.Stop();
// =================== PESQUISA ===================
Console.WriteLine("\n=== PESQUISA ===");
char userOption = 'a';
Dictionary<char, string> options = new Dictionary<char, string>();
options.Add('1', "UF");
options.Add('2', "parte do nome");
options.Add('3', "código IBGE");
while(!options.ContainsKey(userOption))
{
    Console.WriteLine("Digitar uma:\n1-UF\n2=parte do nome\n3-código IBGE:");
    userOption = char.Parse(Console.ReadLine());
}
Console.WriteLine("Digite " + options[userOption]);
string query = Console.ReadLine()?.Trim() ?? "";

List<Municipio> resultados = new List<Municipio>();
switch(userOption)
{
    case '1':
        resultados = municipios.Where(m => m.Uf.Equals(query, StringComparison.OrdinalIgnoreCase)).ToList();
        break;
    case '2':
        resultados = municipios.Where(m => m.NomePreferido.Contains(query)).ToList();
        break;
    case '3':
        resultados = municipios.Where(m => m.Ibge.Equals(query, StringComparison.OrdinalIgnoreCase)).ToList();
        break;
}

if (resultados.Count == 0)
{
    Console.WriteLine("Nenhum município encontrado.");
}
else
{
    Console.WriteLine($"Encontrados {resultados.Count} resultados:");
    foreach (var m in resultados.Take(20)) // limita a 20 na tela
        Console.WriteLine($"{m.Ibge} - {m.NomePreferido} ({m.Uf})");
}
Console.WriteLine($"\nTempo total: {FormatTempo(sw.ElapsedMilliseconds)} ({sw.Elapsed})");
