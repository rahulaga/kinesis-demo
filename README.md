Modified demo from various sources - pluralsight, twitter, kinesis docs.

###Setup
1. Create a Stream with the name "tweets-stream" with 2 shared in Kinesis in the AWS console
2. Create a API user in AWS IAM and setup local aws cli with `aws configure`
```
% aws configure
AWS Access Key ID [****************]: 
AWS Secret Access Key [****************]: 
Default region name [us-east-1]: 
Default output format [json]: 

```
3. Signup as Twitter developer and set local env variable BEARER_TOKEN

###Execute
1. TwitterSampleStreamProducer - streams sample tweets to Kinesis
2. TwitterStreamConsumer - Kinesis reader that periodically polls a shard
3. TwitterFanOutConsumer - Using Kinesis push model instead

###Cleanup
1. Delete the stream, it will also delete the consumer