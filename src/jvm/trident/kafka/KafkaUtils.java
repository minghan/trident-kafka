package trident.kafka;

import backtype.storm.utils.Utils;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;
import trident.kafka.KafkaConfig.KafkaHosts;

public class KafkaUtils {

    public static Map emitPartitionBatchNew(KafkaConfig config, int partition, SimpleConsumer consumer, TransactionAttempt attempt, TridentCollector collector, Map lastMeta, String topologyInstanceId) {
        KafkaHosts hosts = config.hosts;
        int localPartitionId = hosts.getHostPartitionIdByGlobalPartitionId(partition);
        long offset;
        if (lastMeta != null) {
            if(config.forceFromStart && !topologyInstanceId.equals(lastMeta.get("instanceId"))) {
                offset = consumer.getOffsetsBefore(config.topic, localPartitionId, config.startOffsetTime, 1)[0];
            } else {
                offset = (Long) lastMeta.get("nextOffset");                 
            }
        } else {
            long startTime = -1;
            if(config.forceFromStart) startTime = config.startOffsetTime;
            offset = consumer.getOffsetsBefore(config.topic, localPartitionId, startTime, 1)[0];
        }
        ByteBufferMessageSet msgs;
        try {
            msgs = consumer.fetch(new FetchRequest(config.topic, localPartitionId, offset, config.fetchSizeBytes));
        } catch(Exception e) {
            if(e instanceof ConnectException) {
                throw new FailedFetchException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
        long endoffset = offset;
        for(MessageAndOffset msg: msgs) {
            emit(config, attempt, collector, msg.message());
            endoffset = msg.offset();
        }
        Map newMeta = new HashMap();
        newMeta.put("offset", offset);
        newMeta.put("nextOffset", endoffset);
        newMeta.put("instanceId", topologyInstanceId);
        return newMeta;
    }

    public static void emit(KafkaConfig config, TransactionAttempt attempt, TridentCollector collector, Message msg) {
        List<Object> values = config.scheme.deserialize(Utils.toByteArray(msg.payload()));
        if(values!=null) {
            collector.emit(values);
        }
    }
}
