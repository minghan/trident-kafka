package trident.kafka;

import java.io.Serializable;

public class HostPort implements Serializable {
    public final static int DEFAULT_KAFKA_PORT = 9092;

    public String host;
    public int port;

    public HostPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public HostPort(String host) {
        this(host, DEFAULT_KAFKA_PORT);
    }

    @Override
    public boolean equals(Object o) {
        HostPort other = (HostPort) o;
        return host.equals(other.host) && port == other.port;
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
