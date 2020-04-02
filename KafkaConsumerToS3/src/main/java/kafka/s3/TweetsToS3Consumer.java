package kafka.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;

public class TweetsToS3Consumer {

    private static JsonParser parser = new JsonParser();
    private static String extractIdFromTweet(String tweet){
        return parser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){

        String bootstrapServers = "3.82.109.17:9092";
        String groupId = "my_third_app";
        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");

        // create consumer
        KafkaConsumer<String, String> consumer  = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static boolean uploadFile(String fileName, String uploadFilePath) {
        String id = "AKIAZ2DZ3K2JWWTXTTA6";
        String secret = "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";
        String bucketName = "rajasinghr-twitter-sink/twitter-files";
        String fileObjectName = fileName;
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(id, secret);
        try {
            AmazonS3 client = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                    .build();
            client.putObject(new PutObjectRequest(bucketName, fileObjectName, new File(uploadFilePath)));
            return true;
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            return false;
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
                    "means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            return false;
        }
    }

    private static FileWriter file;
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(TweetsToS3Consumer.class.getName());
        KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");

        while(true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(10000));
            Integer recCount = records.count();


            logger.info("Received "+recCount+" records.");
            JSONObject obj = new JSONObject();
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddhhmmss");
            LocalDateTime now = LocalDateTime.now();
            String fileName = "file1_"+dtf.format(now)+".json";
            String fileFullPath = "C:/RS/Courses/Kafka/twitter_files/"+fileName;
            for(ConsumerRecord<String, String> record : records){
                //2 strategies
                //1. Kafka specific
                //String id = record.topic()+"_"+record.partition()+"_"+record.offset();

                //2. Twitter specific
                //id = extractIdFromTweet(record.value());
                try {
                    String id = extractIdFromTweet(record.value());
                    obj.put(id,parser.parse(record.value()).getAsJsonObject());

                }
                catch (NullPointerException e){
                    logger.warn("Skipping bad data" + record.value());
                }

            }

            if (recCount>0) {

                file = new FileWriter(fileFullPath);
                file.write(obj.toJSONString());
                file.flush();
                file.close();
                //BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing the offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
//                boolean status = uploadFile(fileName, fileFullPath);
//                if(status){
//                    logger.info("Successfully uploaded into S3");
//                }
//                else{
//                    logger.info("Failed to upload file into S3");
//                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
