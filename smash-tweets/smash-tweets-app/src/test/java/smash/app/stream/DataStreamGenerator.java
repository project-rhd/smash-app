package smash.app.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.kohsuke.args4j.CmdLineException;
import smash.data.tweets.pojo.Tweet;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Yikai Gong
 */

public class DataStreamGenerator {
  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
//    options.parse(args);
    DataStreamGenerator generator = new DataStreamGenerator();
    JobTimer.print(() -> {
      generator.run(options);
      return null;
    });
    System.exit(0);
  }

  protected void run (GeoMesaOptions options) throws IOException, InterruptedException {

    Properties props = new Properties();
    props.put("bootstrap.servers", "scats-1-interface:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName()).setMaster("local[2]").set("spark.ui.port", "4041");
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      JavaRDD<String> rawJson = sc.textFile("hdfs://scats-1-master:9000/tweets/geoTweets_melb_2017.json");
      Dataset<Row> tweetRaw = ss.read().json(rawJson).selectExpr("value.*");
      JavaRDD<String> tweetStrs = tweetRaw.toJSON().toJavaRDD();
      tweetStrs.foreachPartition(tweetIter->{
        Producer<String, String> producer = new KafkaProducer<>(props);
        tweetIter.forEachRemaining(tweetStr->{
          String key = Tweet.fromJSON(tweetStr).getId_str();
          System.out.println(key);
          producer.send(new ProducerRecord<>("tweets", key, tweetStr));
          try {
            Thread.sleep(10l);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
        producer.close();
      });
    }
  }
}
