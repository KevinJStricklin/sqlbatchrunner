using System.Collections.Generic;
using System.Runtime.Serialization;

namespace SqlBatchRunner
{
    [DataContract]
    public class Config
    {
        [DataMember]
        public string ConnectionString { get; set; }

        [DataMember]
        public IEnumerable<ConnectionStringPathAndAttribute> ConnectionStringXmlSearch { get; set; }

        [DataMember]
        public DatabaseCreationScripts DatabaseCreationScripts { get; set; }
    }

    [DataContract]
    public class ConnectionStringPathAndAttribute
    {
        [DataMember]
        public string NodePath { get; set; }

        [DataMember]
        public string Attribute { get; set; }
    }

    [DataContract]
    public class DatabaseCreationScripts
    {
        [DataMember]
        public string CreateScriptFileName { get; set; }

        [DataMember]
        public IEnumerable<string> SchemaAndSeedScriptFileNames { get; set; }
    }
}
