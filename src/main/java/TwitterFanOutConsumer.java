import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.Consumer;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.util.concurrent.CompletableFuture;

/**
 * Push to consumer
 */
public class TwitterFanOutConsumer {
    public static final String FAN_OUT_CONSUMER_NAME = "fan-out-consumer";

    public static void main(String[] args) throws Exception {
        //get stream detail
        final var kinesisSyncClient = KinesisClient.builder().build();
        final var describeStreamResponse = kinesisSyncClient.describeStream(
                DescribeStreamRequest.builder().streamName(TwitterSampleStreamProducer.STREAM_NAME).build());

        //register a consumer and wait for it to be ready
        KinesisAsyncClient kinesisAsyncClient = KinesisAsyncClient.create();
        Consumer consumer = registerConsumer(kinesisAsyncClient, kinesisSyncClient, describeStreamResponse.streamDescription().streamARN());

        StartingPosition startingPosition = StartingPosition.builder()
                .type(ShardIteratorType.TRIM_HORIZON)
                .build();
        SubscribeToShardRequest subscribeToShardRequest = SubscribeToShardRequest.builder()
                .consumerARN(consumer.consumerARN())
                .shardId("shardId-000000000001")
                .startingPosition(startingPosition)
                .build();

        System.out.println("Subscribing to a shard");
        SubscribeToShardResponseHandler recordsHandler = SubscribeToShardResponseHandler.builder()
                .onError(t -> System.err.println("Subscribe to shard error: " + t.getMessage()))
                .subscriber(new RecordsProcessor())
                .build();

        CompletableFuture<Void> future = kinesisAsyncClient.subscribeToShard(subscribeToShardRequest, recordsHandler);
        future.join();
    }

    private static Consumer registerConsumer(KinesisAsyncClient kinesisAsyncClient, KinesisClient kinesisSyncClient, String streamARN)
            throws Exception {
        //cannot duplicate register so search first (so you can run the program again)
        final var listStreamConsumersResponse = kinesisSyncClient.listStreamConsumers(
                ListStreamConsumersRequest.builder().streamARN(streamARN).build());
        final var existingConsumer = listStreamConsumersResponse.consumers().stream().filter(consumer -> consumer.consumerName().equals(FAN_OUT_CONSUMER_NAME)).findFirst();
        if(existingConsumer.isPresent()){
            System.out.println("Using existing consumer: " + existingConsumer.get());
            return existingConsumer.get();
        }
        else{
            RegisterStreamConsumerRequest registerStreamConsumerRequest
                    = RegisterStreamConsumerRequest.builder()
                    .consumerName(FAN_OUT_CONSUMER_NAME)
                    .streamARN(streamARN)
                    .build();

            RegisterStreamConsumerResponse response = kinesisAsyncClient
                    .registerStreamConsumer(registerStreamConsumerRequest).get();
            Consumer consumer = response.consumer();
            System.out.println("Registered consumer: " + consumer);

            waitForConsumerToRegister(kinesisAsyncClient, consumer);
            return consumer;
        }
    }

    private static void waitForConsumerToRegister(KinesisAsyncClient kinesisAsyncClient, Consumer consumer)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        DescribeStreamConsumerResponse consumerResponse;
        do {
            System.out.println("Waiting for enhanced consumer to become active");
            Thread.sleep(500);
            consumerResponse = kinesisAsyncClient.describeStreamConsumer(
                    DescribeStreamConsumerRequest.builder().consumerARN(consumer.consumerARN()).build()).get();
        } while (consumerResponse.consumerDescription().consumerStatus()!= ConsumerStatus.ACTIVE);
    }

    static class RecordsProcessor implements SubscribeToShardResponseHandler.Visitor {
        @Override
        public void visit(SubscribeToShardEvent event) {
            for (Record record : event.records()) {
                TwitterStreamConsumer.processRecord(record);
            }
        }
    }
}
