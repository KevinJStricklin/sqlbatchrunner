using System;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.IO;
using System.Data;
using System.Security.Cryptography;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace SqlBatchRunner
{
    class SqlRunner
    {
        private String connectionString;

        private bool isUnattendedModeEnabled;

        public SqlRunner(String connectionString)
        {
            this.connectionString = connectionString;
            this.isUnattendedModeEnabled = true;
        }

        public void EnableManualMode()
        {
            isUnattendedModeEnabled = false;
        }

        public void Run(String folderPath)
        {
            DataTable filesPreviouslyRun = readControlTable();

            if (filesPreviouslyRun != null)
            {
                var folderInfo = new DirectoryInfo(folderPath);
                var sqlFiles = folderInfo.EnumerateFiles("*.sql").Where(f => !f.Name.StartsWith("!")).OrderBy(f => f.Name);

                foreach (var fileojb in sqlFiles)
                {
                    var fileContent = File.ReadAllText(fileojb.FullName);

                    //  calculate checksum of file contents
                    var cksum = createCkSum(fileContent);

                    Console.Write("{0} : {1}", fileojb.Name, cksum);

                    if (filesPreviouslyRun.AsEnumerable().Any(row => cksum == row.Field<String>("CheckSum")))
                    {
                        Console.WriteLine(" - Previously executed");
                    }
                    else
                    {
                        Console.WriteLine(" - Executing...");
                        runSql(fileojb.Name, fileContent, cksum);
                    }
                }
            }
        }

        public void RunSingleFile(string fileName)
        {
            Console.Write("{0} - Executing...", fileName);

            var fileContent = File.ReadAllText(fileName);

            ExecuteSqlBatch(fileContent);
        }

        public void CreateDatabase(string folderPath, string databaseCreateScriptFileName, IEnumerable<string> schemaAndSeedScripts)
        {
            SqlConnectionStringBuilder scsb = new SqlConnectionStringBuilder(connectionString);

            var targetDb = scsb.InitialCatalog;
            scsb.InitialCatalog = "master";

            using (var connection = new SqlConnection(scsb.ConnectionString))
            {
                bool DatabaseExists = false;

                var commandText = string.Format("IF DB_ID(N'{0}') IS NOT NULL(SELECT CAST(1 AS BIT)) ELSE (SELECT CAST(0 AS BIT))", targetDb);

                using (var command = new SqlCommand(commandText, connection))
                {
                    connection.Open();
                    DatabaseExists = (bool)command.ExecuteScalar();
                }

                if (!DatabaseExists)
                {
                    Console.WriteLine(" Database not found: {0}", targetDb);
                    var dbCreationScript = GetDatabaseCreationScript(folderPath, databaseCreateScriptFileName, targetDb);

                    foreach (var query in Queries(dbCreationScript))
                    {
                        using (var command = new SqlCommand(query, connection))
                        {
                            try
                            {
                                command.ExecuteNonQuery();
                            }
                            catch (SqlException)
                            {
                                Console.WriteLine(query);
                                throw;
                            }
                        }
                    }

                    foreach (var scriptFile in schemaAndSeedScripts)
                    {
                        Console.WriteLine(" Processing post creation script: {0}", scriptFile);

                        var fileContent = File.ReadAllText(Path.Combine(folderPath, scriptFile));
                        ExecuteSqlBatch(fileContent);
                    }
                }
            }
        }

        //void CreateDatabaseIfNotExist(string folderPath)
        //{
        //    SqlConnectionStringBuilder scsb = new SqlConnectionStringBuilder(connectionString);

        //    var targetDb = scsb.InitialCatalog;
        //    scsb.InitialCatalog = "master";

        //    using (var connection = new SqlConnection(scsb.ConnectionString))
        //    {
        //        bool DatabaseExists = false;

        //        var commandText = string.Format("IF DB_ID(N'{0}') IS NOT NULL(SELECT CAST(1 AS BIT)) ELSE (SELECT CAST(0 AS BIT))", targetDb);

        //        using (var command = new SqlCommand(commandText, connection))
        //        {
        //            connection.Open();
        //            DatabaseExists = (bool)command.ExecuteScalar();
        //        }

        //        if (!DatabaseExists)
        //        {
        //            Console.WriteLine(" Database not found: {0}", targetDb);
        //            var dbCreationScript = GetDatabaseCreationScript(folderPath,  targetDb);

        //            foreach (var query in Queries(dbCreationScript))
        //            {
        //                using (var command = new SqlCommand(query, connection))
        //                {
        //                    try
        //                    {
        //                        command.ExecuteNonQuery();
        //                    }
        //                    catch (SqlException)
        //                    {
        //                        Console.WriteLine(query);
        //                        throw;
        //                    }
        //                }
        //            }
        //        }
        //    }
        //}

        private static string GetDatabaseCreationScript(string folderPath, string databaseCreationScriptFileName, string targetDbName)
        {
            var dbCreationScriptName = Path.Combine(folderPath, databaseCreationScriptFileName);

            //if (File.Exists(dbCreationScriptName))
            //{
                Console.WriteLine(" Processing database creation script: {0}", databaseCreationScriptFileName);
                var fileContent = File.ReadAllText(dbCreationScriptName);

                return DoTokenReplacement(fileContent, targetDbName);
            //}
            //else
            //    throw new Exception(string.Format("Database create script not found: {0}", databaseCreationScriptFileName));
        }

        void ExecuteSqlBatch(string sqlBatch)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                foreach (var query in Queries(sqlBatch))
                {
                    using (var command = new SqlCommand(query, connection))
                    {
                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch (SqlException)
                        {
                            Console.WriteLine(query);
                            throw;
                        }
                    }
                }
            }
        }

        void runSql(String fileName, String fileContent, String cksum)
        {
            if (isUnattendedModeEnabled || ConfirmToContinue(" Execute SQL"))
            {
                ExecuteSqlBatch(fileContent);
            }

            //  log the filename in table
            if (isUnattendedModeEnabled || ConfirmToContinue(" Update tracking table"))
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    var commandText = String.Format("insert into [dbo].[SqlBatchControl] (OriginalFileName, CheckSum, Connection) values ('{0}', '{1}', '{2}')", fileName, cksum, connection.Database);
                    using (var command = new SqlCommand(commandText, connection))
                    {
                        connection.Open();
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        static bool ConfirmToContinue(string v)
        {
            bool check = false;
            ConsoleKeyInfo ck;
            do
            {
                Console.Write("\r{0}? (y/n)  \b", v);
                ck = Console.ReadKey();
                check = !((ck.Key == ConsoleKey.Y) || (ck.Key == ConsoleKey.N));
            } while (check);

            Console.WriteLine();

            return ck.Key == ConsoleKey.Y;
        }

        DataTable readControlTable()
        {
            DataTable fileDataTable = null;

            var sqlCommandText = @"if object_id(N'dbo.SqlBatchControl') is null 
                                        create table dbo.SqlBatchControl ( 
	                                        OriginalFileName varchar(max) not null, 
                                            CheckSum varchar(max) not null,
                                            Connection varchar(max) not null,
                                            UtcDateRun datetime not null default (getutcdate())
                                        )
                                   select OriginalFileName, CheckSum from SqlBatchControl";

            using (var connection = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand(sqlCommandText, connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        fileDataTable = new DataTable();
                        fileDataTable.Load(reader);
                    }
                }
            }

            return fileDataTable;
        }

        static String createCkSum(String filetext)
        {
            byte[] filetextBytes = Encoding.UTF8.GetBytes(filetext);

            using (var md5 = MD5.Create())
            {
                return BitConverter.ToString(md5.ComputeHash(filetextBytes)).Replace("-", string.Empty);
            }
        }

        static IEnumerable<string> Queries(string queryfile)
        {
            List<string> queries = new List<string>();

            using (var reader = new StringReader(queryfile))
            {
                StringBuilder qb = new StringBuilder();
                string line = null;

                while ((line = reader.ReadLine()) != null)
                {
                    if (line.Trim().Equals("go", StringComparison.OrdinalIgnoreCase))
                    {
                        if (qb.Length > 0)
                        {
                            queries.Add(qb.ToString());
                            qb.Clear();
                        }
                    }
                    else if (!string.IsNullOrWhiteSpace(line) || qb.Length > 0)
                        qb.AppendLine(line);
                }

                if (qb.Length > 0)
                    queries.Add(qb.ToString());
            }

            return queries;
        }

        static string DoTokenReplacement(string queryfile, string newValue)
        {
            var regexPattern = @"\${2}(?<database_name_token>\w+)\${2}";
            var regex = new Regex(regexPattern);
            var m = regex.Match(queryfile);

            if (m.Success)
            {
                var oldValue = m.Groups["database_name_token"].Value;

                if (!oldValue.Equals(newValue, StringComparison.Ordinal))
                {
                    Console.WriteLine("  Replacing token {0} with {1}", oldValue, newValue);
                    queryfile = queryfile.Replace(oldValue, newValue);
                }
            }
            return queryfile;
        }
    }
}
