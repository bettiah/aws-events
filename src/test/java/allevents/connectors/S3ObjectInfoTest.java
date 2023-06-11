package allevents.connectors;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class S3ObjectInfoTest {

    @Test
    void fromJsonString() {

        String js = """
                {
                  "Type" : "Notification",
                  "MessageId" : "9ed954be-d6f9-5d94-b913-0845eb627e05",
                  "TopicArn" : "arn:aws:sns:eu-west-1:577014115786:all-events-trail-sns",
                  "Message" : "{\\"s3Bucket\\":\\"all-events-trail-577014115786-eu-west-1-bucket\\",\\"s3ObjectKey\\":[\\"AWSLogs/577014115786/CloudTrail/eu-west-1/2023/06/08/577014115786_CloudTrail_eu-west-1_20230608T0225Z_3o2KZ0eW5pNYtEx7.json.gz\\"]}",
                  "Timestamp" : "2023-06-08T02:22:19.902Z",
                  "SignatureVersion" : "1",
                  "Signature" : "YhsBktP/vEkxyB/DwnJsY/1YEhurX1ENVNXtqp4i2IBhrrgrEUQzQXyFdP7FPYLT1dGGwLnWmn10oceFUCpNR+2SAEaUONJfDE+gUlHONO2sWny7+AUJsnMXwRTd2ShhbSxKia1AnXDE5f1WaJbePwwudwL5RJuh3lr24ZDhgUmIRU8ZAO17VgcoGrFMqWkJYsas0D+CsIR5BnqhNiPRp214lSTqLIoyW/9Ycj78cDFfDR95v1H5MKXhlG8UbTZGzUf4jJAf75q+Az1VAZN4NirL+/oExiCqfyoU09aljSxE2Fld4a3KI2BjFosQK1cEDjJpqTEz/FLCoGoRbpEYTA==",
                  "SigningCertURL" : "https://sns.eu-west-1.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem",
                  "UnsubscribeURL" : "https://sns.eu-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-1:577014115786:all-events-trail-sns:03c6bd7b-a699-4367-a855-d611fdcb9807"
                }
                """;

        ObjectMapper objectMapper = new ObjectMapper();
        S3ObjectInfo s3 = assertDoesNotThrow(() -> S3ObjectInfo.fromJsonString(objectMapper, js));

        assertAll(() -> {
            assertEquals("9ed954be-d6f9-5d94-b913-0845eb627e05", s3.getMessageId());
            assertEquals(List.of("AWSLogs/577014115786/CloudTrail/eu-west-1/2023/06/08/577014115786_CloudTrail_eu-west-1_20230608T0225Z_3o2KZ0eW5pNYtEx7.json.gz"), s3.getS3ObjectKey());
        });
    }
}