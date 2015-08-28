using System;
using System.IO;
using System.Xml.Linq;
using System.Xml.XPath;
using System.Runtime.Serialization.Json;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlBatchRunner
{
    public class ConfigScanner
    {
        private string xmlFile;
        private bool configFound;
        private bool isUnattendedModeEnabled;

        public ConfigScanner(string xmlFileName)
        {
            xmlFile = xmlFileName;
            configFound = false;
            isUnattendedModeEnabled = true;
        }

        public void EnableManualMode()
        {
            isUnattendedModeEnabled = false;
        }

        public bool ProcessDirectory(string directoryName)
        {
            ProcessSingleDirectory(directoryName);

            foreach (var dir in Directory.EnumerateDirectories(directoryName, "*", SearchOption.AllDirectories))
            {
                ProcessDirectory(dir);
            }

            return configFound;
        }

        public bool ProcessFile(string directoryName, string fileName)
        {
            var configFound = false;

            var filename = Path.Combine(directoryName, "config.json");
            if (File.Exists(filename))
            {
                configFound = true;

                var connectionString = GetConnectionString(directoryName);

                var config = GetConfig(directoryName);

                var sqlParameters = GetSqlParameters(config);

                var runner = new SqlRunner(connectionString, sqlParameters);

                runner.RunSingleFile(fileName);

                Console.WriteLine();
            }
            return configFound;
        }

        void ProcessSingleDirectory(string directoryName)
        {
            var filename = Path.Combine(directoryName, "config.json");
            if (File.Exists(filename))
            {
                configFound = true;

                Console.WriteLine("Processing directory {0}", directoryName);

                var connectionString = GetConnectionString(directoryName);

                var config = GetConfig(directoryName);

                var sqlParameters = GetSqlParameters(config);

                var runner = new SqlRunner(connectionString, sqlParameters);
                if (!isUnattendedModeEnabled)
                    runner.EnableManualMode();

                if (config.DatabaseCreationScripts != null && !string.IsNullOrEmpty(config.DatabaseCreationScripts.CreateScriptFileName))
                    runner.CreateDatabase(directoryName, config.DatabaseCreationScripts.CreateScriptFileName, config.DatabaseCreationScripts.SchemaAndSeedScriptFileNames ?? new string[] { });

                runner.Run(directoryName);

                Console.WriteLine();
            }
        }

        static Config GetConfig(string dirName)
        {
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(Config));

            using (var stream = File.OpenRead(Path.Combine(dirName, "config.json")))
            {
                return (Config)serializer.ReadObject(stream);
            }
        }

        SqlParameter[] GetSqlParameters(Config config)
        {
            var sqlParameters = new List<SqlParameter>();

            var xml = XDocument.Load(xmlFile);

            foreach (var search in config.SqlParameterXmlSearch)
            {
                var node = xml.XPathSelectElement(search.NodePath);
                if (node != null)
                {
                    var ParameterName = search.Name;
                    var ParameterValue = node.Attribute(search.Attribute).Value;
                    sqlParameters.Add(new SqlParameter(ParameterName, ParameterValue));
                }
            }

            return sqlParameters.ToArray();
        }

        string GetConnectionString(string dirName)
        {
            string result = null;

            var config = GetConfig(dirName);

            if (config.ConnectionStringXmlSearch != null)
                result = GetConnectionStringFromXML(xmlFile, config.ConnectionStringXmlSearch);

            if (string.IsNullOrEmpty(result))
                result = config.ConnectionString;

            if (string.IsNullOrWhiteSpace(result))
                throw new ArgumentException("ConnectionString not found");

            return result;
        }

        static string GetConnectionStringFromXML(string xmlFile, IEnumerable<ConnectionStringPathAndAttribute> searchValues)
        {
            string result = null;

            var xml = XDocument.Load(xmlFile);

            foreach (var search in searchValues)
            {
                var node = xml.XPathSelectElement(search.NodePath);
                if (node != null)
                {
                    result = node.Attribute(search.Attribute).Value;
                    break;
                }
            }

            return result;
        }

        static public void GenerateSampleConfig(string filename)
        {
            var c = new Config();
            c.ConnectionString = "connection string";
            c.DatabaseCreationScripts = new DatabaseCreationScripts();
            c.DatabaseCreationScripts.CreateScriptFileName = "!Create Database.sql";
            c.DatabaseCreationScripts.SchemaAndSeedScriptFileNames = new List<string> { "!Schema.sql", "!Seed Data.sql" };

            DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(Config));

            using (var stream = File.Create(filename))
            {
                serializer.WriteObject(stream, c);
            }
        }
    }
}
