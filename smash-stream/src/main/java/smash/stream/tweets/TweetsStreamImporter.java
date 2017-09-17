package smash.stream.tweets;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.data.tweets.pojo.Tweet;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaFeatureWriter;
import smash.utils.geomesa.GeoMesaOptions;
import smash.utils.streamTasks.StreamTaskWriter;
import smash.utils.streamTasks.ingest.SFIngestTask;

import java.io.IOException;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class TweetsStreamImporter {
  private static Logger logger = LoggerFactory.getLogger(TweetsStreamImporter.class);
  private SparkConf sparkConf;

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);
    TweetsStreamImporter importer = new TweetsStreamImporter();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  public TweetsStreamImporter(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.files.maxPartitionBytes", "33554432"); // 32MB
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
  }

  public TweetsStreamImporter() {
    this(new SparkConf());
  }

  // TODO need new options for including kafka parameters
  public void run(GeoMesaOptions options) throws InterruptedException, IOException {
    Map<String, String> kafkaParams = new HashMap<>();
    Set<String> topicsSet = new HashSet<>();
    kafkaParams.put("metadata.broker.list", "scats-1-interface:9092");
    kafkaParams.put("auto.offset.reset", "smallest");
    topicsSet.add("tweets");

    // Step 1. ensure table/schema has been created in DB
    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);

    // Step 2. Create SparkStreaming context and define the operations.
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
    JavaPairInputDStream<String, String> directKafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        String.class, String.class,
        StringDecoder.class, StringDecoder.class,
        kafkaParams, topicsSet
      );

    // For each data pair in stream: tuple[_1, _2]
    directKafkaStream.toJavaDStream().foreachRDD(tuple2JavaRDD -> {
      tuple2JavaRDD.foreach(tuple -> {
        // Create map data to ingest
        Tweet t = Tweet.fromJSON(tuple._2);
        if (t == null || t.getCoordinates() == null)
          return;
        SimpleFeature sf = TweetsFeatureFactory.createFeature(t);
        Map<String, SimpleFeature> toIngest = new HashMap<>();
        toIngest.put(tuple._1, sf);
        // Create ingest task
        Properties p = new Properties();
        StreamTaskWriter<SimpleFeature> writer =
          GeoMesaFeatureWriter.createOrGetSingleton(options, sf.getFeatureType().getTypeName());
        p.put(SFIngestTask.PROP_WRITER, writer);

        SFIngestTask ingestTask = SFIngestTask.getThreadSingleton(logger, p);
        // Execute task
        ingestTask.doTask(toIngest);
//        System.out.println(t.getId_str());
      });
    });


    // Step 3. Start the SparkStreaming context
    ssc.sparkContext().setLogLevel("ERROR");
    ssc.start();
    ssc.awaitTermination();
  }
}
