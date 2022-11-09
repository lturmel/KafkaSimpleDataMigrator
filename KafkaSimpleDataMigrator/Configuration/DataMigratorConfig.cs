
namespace KafkaSimpleDataMigrator.Configuration
{
    public sealed class KafkaClusterInfo
    {
        public string BootstrapServer { get; set; }
        public string AccessKey { get; set; }
        public string AccessSecret { get; set; }
        public SchemaRegistryInfo SchemaRegistry { get; set; }
    }

    public sealed class SchemaRegistryInfo
    {
        public string Url { get; set; }
        public string ApiKey { get; set; }
        public string ApiSecret { get; set; }
    }

    public sealed class TopicRenamePattern
    {
        public string RevokePattern { get; set; }
        public string NewPattern { get; set; }
    }

    public sealed class KafkaDataMigratorConfig
    {
        public TopicRenamePattern TopicRenamePattern { get; set; }
        public KafkaClusterInfo SourceCluster { get; set; }
        public KafkaClusterInfo DestinationCluster { get; set; }
    }
}
