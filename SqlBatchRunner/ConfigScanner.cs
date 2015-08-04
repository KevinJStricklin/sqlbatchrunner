using System;
using System.IO;
using System.Xml.Linq;
using System.Xml.XPath;
using System.Runtime.Serialization.Json;
using System.Collections.Generic;

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

        void ProcessSingleDirectory(string directoryName)
        {
            var filename = Path.Combine(directoryName, "config.json");
            if (File.Exists(filename))
            {
                configFound = true;

                Console.WriteLine("Processing directory {0}", directoryName);

                var connectionString = GetConnectionString(directoryName);

                var runner = new SqlRunner(connectionString);
                if (!isUnattendedModeEnabled)
                    runner.EnableManualMode();        
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
    }
}
