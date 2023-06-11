package allevents.connectors.sqs;

import java.util.Objects;

public final class SqsMessage {
    private String messageId;
    private String body;
    private String receiptHandle;

    public SqsMessage(String messageId, String body, String receiptHandle) {
        this.messageId = messageId;
        this.body = body;
        this.receiptHandle = receiptHandle;
    }

    public SqsMessage() {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (SqsMessage) obj;
        return Objects.equals(this.messageId, that.messageId) && Objects.equals(this.body, that.body) && Objects.equals(this.receiptHandle, that.receiptHandle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, body, receiptHandle);
    }

    @Override
    public String toString() {
        return "SQSMessage[" + "messageId=" + messageId + ", " + "body=" + body + ", " + "receiptHandle=" + receiptHandle + ']';
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

}
