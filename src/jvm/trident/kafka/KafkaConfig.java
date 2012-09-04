package trident.kafka;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public interface KafkaHosts {
        public int getTotalNumHosts();
        /** Combined number of partitions across all hosts */
        public int getTotalNumPartitions();
        public HostPort getHostPortByGlobalPartitionId(int partitionId);
        public int getHostPartitionIdByGlobalPartitionId(int partitionId);
    }

    public static class StaticHosts implements Serializable, KafkaHosts {
        private static final long serialVersionUID = 1L;
        private List<HostPort> hosts;
        private int partitionsPerHost;
        
        public static StaticHosts fromHostString(List<String> hostStrings, int partitionsPerHost) {
            return new StaticHosts(convertHosts(hostStrings), partitionsPerHost);
        }

        public StaticHosts(List<HostPort> hosts, int partitionsPerHost) {
            this.hosts = hosts;
            this.partitionsPerHost = partitionsPerHost;
        }

        @Override
        public int getTotalNumPartitions() {
            return this.partitionsPerHost * this.hosts.size();
        }

        @Override
        public int getTotalNumHosts() {
            return this.hosts.size();
        }

        @Override
        public HostPort getHostPortByGlobalPartitionId(int partitionId) {
            return hosts.get(partitionId/this.partitionsPerHost);
        }

        @Override
        public int getHostPartitionIdByGlobalPartitionId(int partitionId) {
            return partitionId % this.partitionsPerHost;
        }
    }

    KafkaHosts hosts;
    public int fetchSizeBytes = 1024*1024;
    public int socketTimeoutMs = 10000;
    public int bufferSizeBytes = 1024*1024;
    public Scheme scheme = new RawScheme();
    public String topic;
    public long startOffsetTime = -2;
    public boolean forceFromStart = false;
    public IBatchCoordinator coordinator = new DefaultCoordinator();

    public KafkaConfig(StaticHosts hosts, String topic) {
        this.hosts = hosts;
        this.topic = topic;
    }

    public void forceStartOffsetTime(long millis) {
        startOffsetTime = millis;
        forceFromStart = true;
    }

    public static HostPort convertHost(String host) {
        HostPort hp;
        String[] spec = host.split(":");
        if(spec.length==1) {
            hp = new HostPort(spec[0]);
        } else if (spec.length==2) {
            hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
        } else {
            throw new IllegalArgumentException("Invalid host specification: " + host);
        }
        return hp;
    }

    public static List<HostPort> convertHosts(List<String> hosts) {
        List<HostPort> ret = new ArrayList<HostPort>();
        for(String s: hosts) {
            ret.add(convertHost(s));
        }
        return ret;
    }
}
