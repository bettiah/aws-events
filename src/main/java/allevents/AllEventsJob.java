package allevents;

import allevents.connectors.S3ObjectInfo;
import allevents.connectors.sqs.SqsConfig;
import allevents.connectors.sqs.SqsSource;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class AllEventsJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setClosureCleanerLevel(ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL);
        env.getConfig().disableGenericTypes();

        final ParameterTool parameters = ParameterTool.fromPropertiesFile("job.properties");
        env.getConfig().setGlobalJobParameters(parameters);

        final SqsSource sqsSource = new SqsSource(
                new SqsConfig(
                        parameters.getInt("sqs.queue.batch", 10),
                        Duration.ofMillis(parameters.getInt("sqs.queue.poll.ms", 100)),
                        parameters.getRequired("sqs.queue.topic"),
                        Duration.ofSeconds(parameters.getInt("sqs.queue.initial-delay.sec", 5)),
                        Duration.ofSeconds(parameters.getInt("sqs.queue.check.sec", 5)))
        );

        final DataStream<S3ObjectInfo> sqsMessages = env
                .addSource(sqsSource)
                .name("sqs")
                .returns(S3ObjectInfo.typeInformation);

        sqsMessages.print();

        env.execute("AllEvents");
    }
}
