import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class TwitterStreamConsumer {
    public static void main(String[] args) {
        final var kinesisClient = KinesisClient.builder().build();

        //reading a specific shard from beginning
        GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder()
                .streamName(TwitterSampleStreamProducer.STREAM_NAME).shardId("shardId-000000000001").shardIteratorType("TRIM_HORIZON").build();

        final var shardIteratorResponse = kinesisClient.getShardIterator(getShardIteratorRequest);
        String shardIterator = shardIteratorResponse.shardIterator();

        while (true) {
            GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder().shardIterator(shardIterator).build();

            final var result = kinesisClient.getRecords(getRecordsRequest);

            List<Record> records = result.records();

            for (Record record : records) {
                processRecord(record);
            }

            sleep(200);

            shardIterator = result.nextShardIterator();
        }

    }

    private static void sleep(long ms) {
        System.out.println("Sleeping");
        try {
            Thread.sleep(ms);
        } catch (InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static void processRecord(Record record) {
        String tweetJson = new String(record.data().asByteArray(), StandardCharsets.UTF_8);

        var tweet = parseTweet(tweetJson);
        if(tweet.isPresent()) {
            System.out.println(tweet.get().getLang() + " => " + tweet.get().getText());
        }
    }

    private static Optional<Status> parseTweet(String tweetJson) {
        try {
            return Optional.of(TwitterObjectFactory.createStatus(tweetJson));
        } catch (TwitterException e) {
            System.err.println("Received bad string:"+tweetJson);
            //ignore
            return Optional.empty();
        }
    }
}
