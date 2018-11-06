using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using DatabaseSchemaReader;
using DatabaseSchemaReader.DataSchema;
using DatabaseSchemaReader.SqlGen;

namespace Vima.SqlServer.Schema
{
    class Program
    {
        private const string ConnectionString = @"Data Source=.\SQLEXPRESS;Integrated Security=true;Initial Catalog=Northwind";
        private static readonly List<string> DefaultSchemasForSqlServer2017 = new List<string>
        {
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
        };

        static void Main()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                var databaseSchema = GetDatabaseSchema(connection);

                string schemas = GenerateCreateSchemaStatements(databaseSchema);
                string tables = GenerateCreateTableStatements(databaseSchema);
                OutputToFile("Tables.sql", schemas + tables);

                string views = GenerateCreateViewStatements(databaseSchema);
                OutputToFile("Views.sql", views);

                string functions = GenerateCreateFunctionStatements(databaseSchema);
                string storedProcedures = GenerateCreateStoredProcedureStatements(databaseSchema);
                OutputToFile("Programmability.sql", functions + storedProcedures);
            }
        }

        private static DatabaseSchema GetDatabaseSchema(DbConnection connection)
        {
            var dbReader = new DatabaseReader(connection);
            dbReader.AllSchemas();
            return dbReader.ReadAll();
        }

        private static void OutputToFile(string fileName, string text)
        {
            if (string.IsNullOrWhiteSpace(fileName) || string.IsNullOrWhiteSpace(text))
            {
                return;
            }

            var currentDirectory = Directory.GetCurrentDirectory();
            var directory = Directory.GetParent(currentDirectory)?.Parent?.Parent;
            if (directory == null)
            {
                return;
            }

            var outputFilePath = Path.Combine(directory.FullName, fileName);
            File.WriteAllText(outputFilePath, text);
        }

        private static string GenerateCreateSchemaStatements(DatabaseSchema databaseSchema)
        {
            var customSchemas = databaseSchema.Schemas
                .Where(x => !DefaultSchemasForSqlServer2017.Contains(x.Name))
                .OrderBy(x => x.Name).ToList();
            if (customSchemas.Count == 0) return string.Empty;

            StringBuilder stringBuilder = new StringBuilder();
            foreach (var schema in customSchemas)
            {
                stringBuilder.AppendLine($"CREATE SCHEMA [{schema.Name}]\nGO\n");
            }

            stringBuilder.AppendLine();
            return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
        }

        private static string GenerateCreateTableStatements(DatabaseSchema databaseSchema)
        {
            if (databaseSchema.Tables.Count == 0) return string.Empty;

            var tablesGenerator = new DdlGeneratorFactory(SqlType.SqlServer).AllTablesGenerator(databaseSchema);
            tablesGenerator.IncludeSchema = true;
            return RemoveMultipleConsecutiveNewLineCharacters(tablesGenerator.Write());
        }

        private static string GenerateCreateViewStatements(DatabaseSchema databaseSchema)
        {
            var views = databaseSchema.Views;
            if (views.Count == 0) return string.Empty;

            StringBuilder stringBuilder = new StringBuilder();
            foreach (var view in views.OrderBy(x => x.DatabaseSchema).ThenBy(x => x.Name))
            {
                stringBuilder.AppendLine(view.Sql + "\nGO\n");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine();
            return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
        }

        private static string GenerateCreateFunctionStatements(DatabaseSchema databaseSchema)
        {
            var functions = databaseSchema.Functions;
            if (functions.Count == 0) return string.Empty;

            StringBuilder stringBuilder = new StringBuilder();
            foreach (var function in functions.OrderBy(x => x.DatabaseSchema).ThenBy(x => x.Name))
            {
                stringBuilder.AppendLine(function.Sql + "\nGO\n");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine();
            return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
        }

        private static string GenerateCreateStoredProcedureStatements(DatabaseSchema databaseSchema)
        {
            var storedProcedures = databaseSchema.StoredProcedures;
            if (storedProcedures.Count == 0) return string.Empty;

            StringBuilder stringBuilder = new StringBuilder();
            foreach (var storedProcedure in storedProcedures.OrderBy(x => x.DatabaseSchema).ThenBy(x => x.Name))
            {
                stringBuilder.AppendLine(storedProcedure.Sql + "\nGO\n");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine();
            return RemoveMultipleConsecutiveNewLineCharacters(stringBuilder.ToString());
        }

        private static string RemoveMultipleConsecutiveNewLineCharacters(string source)
        {
            string textWithStandardizedNewLineCharacter = Regex.Replace(source, @"(?:\r\n|[\r\n])", "\n");
            string[] listOfLinesWithoutNewLineCharacters = Regex.Split(textWithStandardizedNewLineCharacter, "\n{2,}");
            return string.Join("\n\n", listOfLinesWithoutNewLineCharacters);
        }
    }
}