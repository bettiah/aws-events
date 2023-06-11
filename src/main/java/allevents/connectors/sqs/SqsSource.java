package allevents.connectors.sqs;

import allevents.connectors.S3ObjectInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.IntStream;


public class SqsSource extends RichSourceFunction<S3ObjectInfo> {
    private static final Logger logger = LoggerFactory.getLogger(SqsSource.class);
    private final SqsConfig sqsConfig;
    private transient SqsClient sqsClient;
    private boolean isRunning;
    private transient ScheduledExecutorService queryService;
    private transient ScheduledFuture<?> queryTask;
    private transient ScheduledFuture<?> receiptTask;

    public SqsSource(SqsConfig sqsConfig) {
        this.sqsConfig = sqsConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sqsClient = SqsClient.builder()
                .httpClientBuilder(ApacheHttpClient.builder())
                .credentialsProvider(AWSGeneralUtil.getCredentialsProvider(Map.of()))
                .build();
    }


    private void pollLoop(BlockingQueue<SqsMessage> pipe, BlockingQueue<String> receiptPipe, SourceContext<S3ObjectInfo> sourceContext) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();

        while (isRunning) {
            SqsMessage message = pipe.poll(sqsConfig.getPollIntervalMs(), TimeUnit.MILLISECONDS);
            if (message != null) {
                try {
                    sourceContext.collect(S3ObjectInfo.fromJsonString(objectMapper, message.getBody()));
                } catch (JsonProcessingException e) {
                    logger.warn("while parsing sqs", e);
                }
                receiptPipe.put(message.getReceiptHandle());
            }
        }
    }

    @Override
    public void run(SourceContext<S3ObjectInfo> ctx) throws Exception {
        queryService = Executors.newSingleThreadScheduledExecutor();

        BlockingQueue<SqsMessage> messagePipe = new LinkedBlockingQueue<>();
        queryTask = queryService.scheduleAtFixedRate(
                new SQSPollerTask(messagePipe, sqsClient, sqsConfig),
                sqsConfig.getInitialDelayMs(),
                sqsConfig.getCheckIntervalMs(),
                TimeUnit.MILLISECONDS);

        BlockingQueue<String> receiptPipe = new LinkedBlockingQueue<>();
        receiptTask = queryService.scheduleAtFixedRate(
                new SQSReceiptTask(receiptPipe, sqsClient, sqsConfig),
                sqsConfig.getInitialDelayMs() + sqsConfig.getCheckIntervalMs(),
                sqsConfig.getCheckIntervalMs(),
                TimeUnit.MILLISECONDS);

        isRunning = true;
        try {
            pollLoop(messagePipe, receiptPipe, ctx);
        } catch (InterruptedException e) {
            isRunning = false;
            logger.info("Interrupted SQS Source Function");
            throw e;
        }
    }

    @Override
    public void cancel() {
        queryTask.cancel(true);
        receiptTask.cancel(true);
        queryService.shutdown();

        try {
            queryService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Didn't manage to stop threading services in time", e);
            Thread.currentThread().interrupt();
        }

        sqsClient.close();
    }

    private static class SQSPollerTask implements Runnable {
        private final BlockingQueue<SqsMessage> pipe;
        private final SqsClient sqsClient;
        private final SqsConfig config;

        public SQSPollerTask(BlockingQueue<SqsMessage> pipe, SqsClient sqsClient, SqsConfig config) {
            this.pipe = pipe;
            this.sqsClient = sqsClient;
            this.config = config;
        }

        @Override
        public void run() {
            final ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(config.getQueueURL())
                    .maxNumberOfMessages(config.getReadBatchSize())
                    .build();

            logger.debug("poll SQS: {}", receiveMessageRequest);
            List<Message> messages = sqsClient
                    .receiveMessage(receiveMessageRequest)
                    .messages();

            logger.debug("poll SQS result: {}", messages.size());
            messages.forEach(msg -> {
                try {
                    pipe.put(new SqsMessage(msg.messageId(), msg.body(), msg.receiptHandle()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    private static class SQSReceiptTask implements Runnable {
        private final BlockingQueue<String> pipe;
        private final SqsClient sqsClient;
        private final SqsConfig config;

        public SQSReceiptTask(BlockingQueue<String> pipe, SqsClient sqsClient, SqsConfig config) {
            this.pipe = pipe;
            this.sqsClient = sqsClient;
            this.config = config;
        }

        @Override
        public void run() {
            final ArrayList<String> receipts = new ArrayList<>();
            pipe.drainTo(receipts);
            logger.debug("SQS receipts: {}", receipts.size());
            final List<DeleteMessageBatchRequestEntry> entries = IntStream.range(0, receipts.size())
                    .mapToObj(i ->
                            DeleteMessageBatchRequestEntry.builder()
                                    .id(Integer.toString(i)).receiptHandle(receipts.get(i))
                                    .build())
                    .toList();

            final DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder()
                    .queueUrl(config.getQueueURL())
                    .entries(entries)
                    .build();

            final DeleteMessageBatchResponse deleted = sqsClient.deleteMessageBatch(deleteMessageBatchRequest);

            logger.debug("SQS receipts deleted - ok:{}, fail:{}",
                    deleted.successful().stream().map(DeleteMessageBatchResultEntry::id),
                    deleted.failed());
        }

    }

}
