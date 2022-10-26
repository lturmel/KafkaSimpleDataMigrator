# KafkaSimpleDataMigrator

Dummy application used to transfert Kafka Message from Topic 'Source.TopicName' to 'Dest.TopicName'

```json
{
  "KafkaMigratorConfig": {
    "TopicRenamePattern": {
      "RevokePattern": "a.source",
      "NewPattern": "the.destination"
    },
    "SourceCluster": {
      "BootstrapServer": "<<source_cluster>>",
      "AccessKey": "<<source_api_key>>",
      "AccessSecret": "<<source_api_secret>>"
    },
    "DestinationCluster": {
      "BootstrapServer": "<<dest_cluster>>",
      "AccessKey": "<<destination_api_key>>",
      "AccessSecret": "<<destination_api_secret>>"
    }
  }
}
```

### ***Important***: The Application doesn't create topic if they aren't existing on the destination cluster