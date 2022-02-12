import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

/**
 * Modified from https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Sampled-Stream/SampledStream.java
 */
public class TwitterSampleStreamProducer {
    public static final String STREAM_NAME = "tweets-stream";

    // To set your environment variables in your terminal run the following line:
    // export 'BEARER_TOKEN'='<your_bearer_token>'
    public static void main(String args[]) throws IOException, URISyntaxException, TwitterException {
        String bearerToken = System.getenv("BEARER_TOKEN");
        if (null != bearerToken) {
            connectStream(bearerToken, KinesisClient.builder().build());
        } else {
            System.out.println("There was a problem getting your bearer token. Please make sure you set the BEARER_TOKEN environment variable");
        }
    }

    /*
     * This method calls the sample stream endpoint and streams Tweets from it
     * */
    private static void connectStream(String bearerToken, KinesisClient kinesisClient) throws IOException, URISyntaxException, TwitterException {

        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD).build()).build();

        //ref https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream
        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream?tweet.fields=lang");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            while (line != null) {
                //this line is a tweet like {"data":{"id":"14922750","lang":"ja","text":"foo"}}
                final var jsonObject = new JSONObject(line);
                final var statusData = jsonObject.getJSONObject("data").toString();//extract the data
                produceToKinesis(kinesisClient, statusData);

                line = reader.readLine();
            }
        }
    }

    private static void produceToKinesis(KinesisClient kinesisClient, String statusString) throws TwitterException {
        Status status = TwitterObjectFactory.createStatus(statusString);
        System.out.println("Parsed:" + status);

        byte[] tweetsBytes = statusString.getBytes(StandardCharsets.UTF_8);

        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(STREAM_NAME).partitionKey(status.getLang()).data(SdkBytes.fromByteArray(tweetsBytes)).build();

        final var putRecordResponse = kinesisClient.putRecord(putRecordRequest);

        System.out.println(status.getLang() + " -> " + putRecordResponse);
    }
}