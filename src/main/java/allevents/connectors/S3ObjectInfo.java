package allevents.connectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public class S3ObjectInfo {
    public static final TypeInformation<S3ObjectInfo> typeInformation = Types.POJO(
            S3ObjectInfo.class,
            Map.of(
                    "timestamp", Types.STRING,
                    "messageId", Types.STRING,
                    "s3Bucket", Types.STRING,
                    "s3ObjectKey", Types.LIST(Types.STRING)
            )
    );
    private String timestamp;
    private String messageId;
    private String s3Bucket;
    private List<String> s3ObjectKey;

    public S3ObjectInfo(String timestamp, String messageId, String s3Bucket, List<String> s3ObjectKey) {
        this.timestamp = timestamp;
        this.messageId = messageId;
        this.s3Bucket = s3Bucket;
        this.s3ObjectKey = s3ObjectKey;
    }

    public S3ObjectInfo() {
    }

    public static S3ObjectInfo fromJsonString(ObjectMapper objectMapper, String body) throws JsonProcessingException {
        SqsPart sqsMessage = objectMapper.readValue(body, SqsPart.class);
        S3Part s3Part = objectMapper.readValue(sqsMessage.message, S3Part.class);
        return new S3ObjectInfo(
                sqsMessage.timestamp,
                sqsMessage.messageId,
                s3Part.s3Bucket,
                s3Part.s3ObjectKey);
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public List<String> getS3ObjectKey() {
        return s3ObjectKey;
    }

    public void setS3ObjectKey(List<String> s3ObjectKey) {
        this.s3ObjectKey = s3ObjectKey;
    }

    @Override
    public String
    toString() {
        return "S3ObjectInfo{" +
                "timestamp='" + timestamp + '\'' +
                ", messageId='" + messageId + '\'' +
                ", s3Bucket='" + s3Bucket + '\'' +
                ", s3ObjectKey=" + s3ObjectKey +
                '}';
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SqsPart {

        public String timestamp;
        public String messageId;
        public String message;

        @JsonCreator
        public SqsPart(Map<String, Object> delegate) {
            this.timestamp = (String) delegate.get("Timestamp");
            this.messageId = (String) delegate.get("MessageId");
            this.message = (String) delegate.get("Message");
        }
    }

    public static class S3Part {
        public String s3Bucket;
        public List<String> s3ObjectKey;

        @JsonCreator
        public S3Part(Map<String, Object> delegate) {
            this.s3Bucket = (String) delegate.get("s3Bucket");
            this.s3ObjectKey = (List<String>) delegate.get("s3ObjectKey");
        }
    }
}
