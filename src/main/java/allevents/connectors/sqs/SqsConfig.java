package allevents.connectors.sqs;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

public class SqsConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 2405172041950251807L;

    private Integer readBatchSize;
    private Long pollIntervalMs;
    private String queueURL;
    private Long initialDelayMs;
    private Long checkIntervalMs;

    public SqsConfig(
            int readBatchSize,
            Duration pollInterval,
            String queueURL,
            Duration initialDelay,
            Duration checkInterval) {
        this.readBatchSize = readBatchSize;
        this.pollIntervalMs = pollInterval.toMillis();
        this.queueURL = queueURL;
        this.initialDelayMs = initialDelay.toMillis();
        this.checkIntervalMs = checkInterval.toMillis();
    }

    public SqsConfig() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SqsConfig) obj;
        return this.readBatchSize == that.readBatchSize &&
                Objects.equals(this.pollIntervalMs, that.pollIntervalMs) &&
                Objects.equals(this.queueURL, that.queueURL) &&
                Objects.equals(this.initialDelayMs, that.initialDelayMs) &&
                Objects.equals(this.checkIntervalMs, that.checkIntervalMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readBatchSize, pollIntervalMs, queueURL, initialDelayMs, checkIntervalMs);
    }

    @Override
    public String toString() {
        return "SqsConfig[" +
                "readBatchSize=" + readBatchSize + ", " +
                "pollInterval=" + pollIntervalMs + ", " +
                "queueURL=" + queueURL + ", " +
                "initialDelay=" + initialDelayMs + ", " +
                "checkInterval=" + checkIntervalMs + ']';
    }

    public int getReadBatchSize() {
        return readBatchSize;
    }

    public void setReadBatchSize(int readBatchSize) {
        this.readBatchSize = readBatchSize;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public String getQueueURL() {
        return queueURL;
    }

    public void setQueueURL(String queueURL) {
        this.queueURL = queueURL;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public long getCheckIntervalMs() {
        return checkIntervalMs;
    }

    public void setCheckIntervalMs(long checkIntervalMs) {
        this.checkIntervalMs = checkIntervalMs;
    }
}
