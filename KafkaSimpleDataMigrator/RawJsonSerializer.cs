using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaSimpleDataMigrator
{
    public class RawJsonSerializer : IAsyncSerializer<string>
    {
        private const byte __MAGIC_BYTE = 0;
        private const int __DEFAULT_INITIAL_BUFFER_SIZE = 1024;

        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly SubjectNameStrategyDelegate? _subjectNameStrategy = null;

        public RawJsonSerializer(ISchemaRegistryClient schemaRegistryClient, JsonSerializerConfig config)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _subjectNameStrategy = config.SubjectNameStrategy?.ToDelegate()
                ?? throw new ArgumentException($"JsonSerializerConfig.SubjectNameStrategy must be defined");
        }

        public async Task<byte[]> SerializeAsync(string data, SerializationContext context)
        {
            var subject = _subjectNameStrategy!(context, context.Topic);
            var latestSchema = await _schemaRegistryClient.GetLatestSchemaAsync(subject);
            using var stream = new MemoryStream(__DEFAULT_INITIAL_BUFFER_SIZE);
            using var writer = new BinaryWriter(stream);
            stream.WriteByte(__MAGIC_BYTE);
            writer.Write(IPAddress.HostToNetworkOrder(latestSchema.Id));
            writer.Write(System.Text.Encoding.UTF8.GetBytes(data));
            return stream.ToArray();
        }
    }
}