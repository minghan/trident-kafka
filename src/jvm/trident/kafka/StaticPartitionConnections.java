package trident.kafka;

import java.util.HashMap;
import java.util.Map;
import kafka.javaapi.consumer.SimpleConsumer;
import trident.kafka.KafkaConfig.KafkaHosts;

public class StaticPartitionConnections {
    // Mapping of HostPort to SimpleConsumer
    Map<HostPort, SimpleConsumer> consumersMap = new HashMap<HostPort, SimpleConsumer>();
    KafkaConfig _config;
    KafkaHosts hosts;
    
    public StaticPartitionConnections(KafkaConfig conf) {
        _config = conf;
        // TODO Do we need this?
        if(!(conf.hosts instanceof KafkaConfig.StaticHosts)) {
            throw new RuntimeException("Must configure with static hosts");
        }
        this.hosts = conf.hosts;
    }

    public SimpleConsumer getConsumer(int partitionId) {
        HostPort hp = hosts.getHostPortByGlobalPartitionId(partitionId);
        if(!consumersMap.containsKey(hp)) {
            consumersMap.put(hp, new SimpleConsumer(hp.host, hp.port, _config.socketTimeoutMs, _config.bufferSizeBytes));
        }
        return consumersMap.get(hp);
    }

    public void close() {
        for(SimpleConsumer consumer: consumersMap.values()) {
            consumer.close();
        }
    }
}
