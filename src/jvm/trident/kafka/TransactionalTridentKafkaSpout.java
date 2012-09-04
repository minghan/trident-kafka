package trident.kafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import java.util.Map;
import java.util.UUID;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.topology.TransactionAttempt;

public class TransactionalTridentKafkaSpout implements IPartitionedTridentSpout<Map> {

    KafkaConfig _config;
    String _topologyInstanceId = UUID.randomUUID().toString();

    public TransactionalTridentKafkaSpout(KafkaConfig config) {
        _config = config;
    }

    class Coordinator implements IPartitionedTridentSpout.Coordinator {
        @Override
        public long numPartitions() {
            return _config.hosts.getTotalNumPartitions();
        }

        @Override
        public void close() {
            _config.coordinator.close();
        }

        @Override
        public boolean isReady(long txid) {
            return _config.coordinator.isReady(txid);
        }
    }

    class Emitter implements IPartitionedTridentSpout.Emitter<Map> {
        StaticPartitionConnections _connections;

        public Emitter() {
            _connections = new StaticPartitionConnections(_config);
        }

        @Override
        public Map emitPartitionBatchNew(TransactionAttempt attempt, TridentCollector collector, int partition, Map lastMeta) {
            SimpleConsumer consumer = _connections.getConsumer(partition);

            return KafkaUtils.emitPartitionBatchNew(_config, partition, consumer, attempt, collector, lastMeta, _topologyInstanceId);
        }

        @Override
        public void emitPartitionBatch(TransactionAttempt attempt, TridentCollector collector, int partition, Map meta) {
            String instanceId = (String) meta.get("instanceId");
            if(!_config.forceFromStart || instanceId.equals(_topologyInstanceId)) {
                SimpleConsumer consumer = _connections.getConsumer(partition);
                long offset = (Long) meta.get("offset");
                long nextOffset = (Long) meta.get("nextOffset");
                int localPartitionId = _config.hosts.getHostPartitionIdByGlobalPartitionId(partition);
                ByteBufferMessageSet msgs = consumer.fetch(new FetchRequest(_config.topic, localPartitionId, offset, _config.fetchSizeBytes));
                for(MessageAndOffset msg: msgs) {
                    if(offset == nextOffset) break;
                    if(offset > nextOffset) {
                        throw new RuntimeException("Error when re-emitting batch. overshot the end offset");
                    }
                    KafkaUtils.emit(_config, attempt, collector, msg.message());
                    offset = msg.offset();
                }        
            }
        }

        @Override
        public void close() {
            _connections.close();
        }
    }

    @Override
    public IPartitionedTridentSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
        return new Coordinator();
    }

    @Override
    public IPartitionedTridentSpout.Emitter getEmitter(Map conf, TopologyContext context) {
        return new Emitter();
    }

    @Override
    public Fields getOutputFields() {
        return _config.scheme.getOutputFields();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
