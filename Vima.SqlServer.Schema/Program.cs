using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using DatabaseSchemaReader;
using DatabaseSchemaReader.DataSchema;
using DatabaseSchemaReader.SqlGen;

namespace Vima.SqlServer.Schema;

internal class Program
{
    private const string ConnectionString =
        @"Data Source=.\SQLEXPRESS;Integrated Security=true;Initial Catalog=Northwind";

    private static readonly List<string> DefaultSchemasForSqlServer2017 =
    [
        "db_accessadmin",
        "db_backupoperator",
        "db_datareader",
        "db_datawriter",
        "db_ddladmin",
        "db_denydatareader",
        "db_denydatawriter",
        "db_owner",
        "db_securityadmin",
        "dbo",
        "guest",
        "INFORMATION_SCHEMA",
        "sys"
    ];

    private static void Main()
    {
        using SqlConnection connection = new(ConnectionString);
        DatabaseSchema databaseSchema = GetDatabaseSchema(connection);

        string schemas = GenerateCreateSchemaStatements(databaseSchema);
        string tables = GenerateCreateTableStatements(databaseSchema);
        OutputToFile("Tables.sql", schemas + tables);

        string views = GenerateCreateViewStatements(databaseSchema);
        OutputToFile("Views.sql", views);

        string functions = GenerateCreateFunctionStatements(databaseSchema);
        string storedProcedures = GenerateCreateStoredProcedureStatements(databaseSchema);
        OutputToFile("Programmability.sql", functions + storedProcedures);
    }

    private static DatabaseSchema GetDatabaseSchema(DbConnection connection)
    {
        DatabaseReader dbReader = new(connection);
        dbReader.AllSchemas();
        return dbReader.ReadAll();
    }

    private static void OutputToFile(string fileName, string text)
    {
        if (string.IsNullOrWhiteSpace(fileName) || string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        string currentDirectory = Directory.GetCurrentDirectory();
        DirectoryInfo? directory = Directory.GetParent(currentDirectory)?.Parent?.Parent;
        if (directory == null)
        {
            return;
        }

        string outputFilePath = Path.Combine(directory.FullName, fileName);
        File.WriteAllText(outputFilePath, text);
    }

    private static string GenerateCreateSchemaStatements(DatabaseSchema databaseSchema)
    {
        List<DatabaseDbSchema> customSchemas = databaseSchema.Schemas
            .Where(x => !DefaultSchemasForSqlServer2017.Contains(x.Name))
            .OrderBy(x => x.Name)
            .ToList();
        if (customSchemas.Count == 0) return string.Empty;

        StringBuilder stringBuilder = new();
        foreach (DatabaseDbSchema? schema in customSchemas)
        {
            stringBuilder.AppendLine($"CREATE SCHEMA [{schema.Name}]\nGO\n");
        }

        stringBuilder.AppendLine();
        return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
    }

    private static string GenerateCreateTableStatements(DatabaseSchema databaseSchema)
    {
        if (databaseSchema.Tables.Count == 0) return string.Empty;

        ITablesGenerator? tablesGenerator =
            new DdlGeneratorFactory(SqlType.SqlServer).AllTablesGenerator(databaseSchema);
        tablesGenerator.IncludeSchema = true;
        return RemoveMultipleConsecutiveNewLineCharacters(tablesGenerator.Write());
    }

    private static string GenerateCreateViewStatements(DatabaseSchema databaseSchema)
    {
        List<DatabaseView>? views = databaseSchema.Views;
        if (views.Count == 0) return string.Empty;

        StringBuilder stringBuilder = new();
        foreach (DatabaseView? view in views.OrderBy(x => x.DatabaseSchema).ThenBy(x => x.Name))
        {
            stringBuilder.AppendLine(view.Sql + "\nGO\n");
            stringBuilder.AppendLine();
        }

        stringBuilder.AppendLine();
        return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
    }

    private static string GenerateCreateFunctionStatements(DatabaseSchema databaseSchema)
    {
        List<DatabaseFunction>? functions = databaseSchema.Functions;
        if (functions.Count == 0) return string.Empty;

        StringBuilder stringBuilder = new();
        foreach (DatabaseFunction? function in functions.OrderBy(x => x.DatabaseSchema).ThenBy(x => x.Name))
        {
            stringBuilder.AppendLine(function.Sql + "\nGO\n");
            stringBuilder.AppendLine();
        }

        stringBuilder.AppendLine();
        return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
    }

    private static string GenerateCreateStoredProcedureStatements(DatabaseSchema databaseSchema)
    {
        List<DatabaseStoredProcedure>? storedProcedures = databaseSchema.StoredProcedures;
        if (storedProcedures.Count == 0) return string.Empty;

        StringBuilder stringBuilder = new();
        foreach (DatabaseStoredProcedure? storedProcedure in storedProcedures.OrderBy(x => x.DatabaseSchema)
                     .ThenBy(x => x.Name))
        {
            stringBuilder.AppendLine(storedProcedure.Sql + "\nGO\n");
            stringBuilder.AppendLine();
        }

        stringBuilder.AppendLine();
        return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
    }

    private static string RemoveMultipleConsecutiveNewLineCharacters(string source)
    {
#pragma warning disable SYSLIB1045 // Convert to 'GeneratedRegexAttribute'.
        string textWithStandardizedNewLineCharacter = Regex.Replace(source, @"(?:\r\n|[\r\n])", "\n");
        string[] listOfLinesWithoutNewLineCharacters = Regex.Split(textWithStandardizedNewLineCharacter, "\n{2,}");
        return string.Join("\n\n", listOfLinesWithoutNewLineCharacters);
#pragma warning restore SYSLIB1045 // Convert to 'GeneratedRegexAttribute'.
    }
}