using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;
using KafkaSimpleDataMigrator.Configuration;

using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaSimpleDataMigrator
{
    class Program
    {
        private static KafkaDataMigratorConfig _migratorConfig = null;

        private static KafkaDataMigratorConfig Configure()
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();
            var kafkaDataMigratorConfig = new KafkaDataMigratorConfig();
            configuration.GetSection("KafkaMigratorConfig").Bind(kafkaDataMigratorConfig);
            return kafkaDataMigratorConfig;
        }

        static void Main(string[] args)
        {
            Console.Clear();
            _migratorConfig = Configure();
            var sourceTopics = GetTopics(_migratorConfig.SourceCluster);
            sourceTopics.ForEach(x => {
                if(x.Topic.StartsWith(_migratorConfig.TopicRenamePattern.RevokePattern))
                    ReadTopicContent(_migratorConfig.SourceCluster, x);
                else
                    Console.WriteLine($"{x.Topic} Not Considered");
            });
            Console.WriteLine("");
            Console.WriteLine("Exit");
        }

        static ProducerConfig CreateProducerConfig(KafkaClusterInfo cluster)
        {
            return new ProducerConfig
            {
                BootstrapServers = cluster.BootstrapServer,
                SaslUsername = cluster.AccessKey,
                SaslPassword = cluster.AccessSecret,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                ClientId = Environment.MachineName,
            };
        }

        static ConsumerConfig CreateConsumerConfig(KafkaClusterInfo cluster)
        {
            return new ConsumerConfig
            {
                BootstrapServers = cluster.BootstrapServer,
                SaslUsername = cluster.AccessKey,
                SaslPassword = cluster.AccessSecret,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                ClientId = Environment.MachineName,
                EnableAutoCommit = false,
                EnableAutoOffsetStore = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = $"dataMigrator-{Guid.NewGuid()}"
            };
        }

        static SchemaRegistryConfig GetSchemaRegistry(SchemaRegistryInfo schemaRegistryInfo)
        {
            return new SchemaRegistryConfig
            {
                Url = schemaRegistryInfo.Url,
                BasicAuthUserInfo = $"{schemaRegistryInfo.ApiKey}:{schemaRegistryInfo.ApiSecret}"
            };
        }

        static string ParseDestinationTopicName(string topicName, TopicRenamePattern renamePattern)
        {
            if(topicName.StartsWith(renamePattern.RevokePattern))
                return topicName.Replace(renamePattern.RevokePattern, renamePattern.NewPattern);
            return topicName;
        }

        static void ReadTopicContent(KafkaClusterInfo cluster, TopicMetadata topic)
        {
            Console.WriteLine($"Reading {topic.Topic} from {cluster.BootstrapServer}");
            var consumer = new ConsumerBuilder<string,string>(CreateConsumerConfig(cluster)).Build();
            var cancelled = false;
            consumer.Subscribe(topic.Topic);
            while (!cancelled)
            {
                try
                {
                    var consumeResult = consumer.Consume(5000);
                    if (consumeResult == null) { 
                        cancelled = true;
                        return;
                    }
                    var msg = consumeResult.Message;
                    Console.WriteLine($"Consume -> Partition: {consumeResult.Partition.Value} - Offset: {consumeResult.Offset} - Value: {msg.Value} - TS: {msg.Timestamp.UtcDateTime.ToLongTimeString()}");
                    RePublish(_migratorConfig.DestinationCluster, ParseDestinationTopicName(topic.Topic, _migratorConfig.TopicRenamePattern), msg);
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Commit error: {e.Error.Reason}");
                } catch(Exception ex)
                {
                    Console.WriteLine($"{ex.Message}");
                    cancelled = true;
                }
            }
            consumer.Close();
        }

        static List<TopicMetadata> GetTopics(KafkaClusterInfo clusterInfo) {
            var topicList = new List<TopicMetadata>();
            var adminClientConfig = new AdminClientConfig();
            adminClientConfig.BootstrapServers = clusterInfo.BootstrapServer;
            adminClientConfig.SaslUsername = clusterInfo.AccessKey;
            adminClientConfig.SaslPassword = clusterInfo.AccessSecret;
            adminClientConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            adminClientConfig.SaslMechanism = SaslMechanism.Plain;

            using (var adminClient = new AdminClientBuilder(adminClientConfig).Build())
            {
                var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));
                topicList.AddRange(meta.Topics);
            }
            return topicList;
        }

        static void RePublish(KafkaClusterInfo cluster, string topicName, Message<string, string> message)
        {
            var jsonSerConfig = new JsonSerializerConfig
            {
                BufferBytes = 100,
                AutoRegisterSchemas = false,
                UseLatestVersion = true,
                SubjectNameStrategy = SubjectNameStrategy.Topic
            };

            var schemaRegistry = new CachedSchemaRegistryClient(GetSchemaRegistry(cluster.SchemaRegistry));

            using var producer = new ProducerBuilder<string,string>(CreateProducerConfig(cluster))
                .SetValueSerializer(new RawJsonSerializer(schemaRegistry, jsonSerConfig))
                .Build();
            var result = producer.ProduceAsync(topicName, message);
            result.Wait();
            Console.WriteLine($"    Transfered to {topicName}");
        }
    }
}
