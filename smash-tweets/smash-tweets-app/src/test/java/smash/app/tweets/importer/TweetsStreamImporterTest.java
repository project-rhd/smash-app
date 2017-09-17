package smash.app.tweets.importer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kohsuke.args4j.CmdLineException;
import smash.utils.JobTimer;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yikai Gong
 */

public class TweetsStreamImporterTest {
//  private static Logger logger = LoggerFactory.getLogger(TweetsStreamImporter.class);


  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    TweetsImporterOptions options = new TweetsImporterOptions();
//    options.parse(args);
    TweetsStreamImporterTest importer = new TweetsStreamImporterTest();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  protected void run (TweetsImporterOptions options) throws IOException, InterruptedException {
    // Ensures Feature Type is saved in GeoMesa
//    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);

//    System.setProperty("twitter4j.oauth.consumerKey", "WbOlRQ57jyp9vnvb5ADLH5bTR");
//    System.setProperty("twitter4j.oauth.consumerSecret", "VwtIz3DtPcxFhYrk0PuP9yoldyz9Da9Ej6FzHTtNPyHJDNXNLW");
//    System.setProperty("twitter4j.oauth.accessToken", "2467677446-WNTeaJVNyIvgl7aWhyOanh1sXX0PsN7hOww9kfu");
//    System.setProperty("twitter4j.oauth.accessTokenSecret", "DU8kRPStao0lFzkGKNqYlGeojs34lPyttOh4R0dbU5HdW");

    SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName()).setMaster("local[2]");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.milliseconds(10l));
    JavaDStream<String> tweets = ssc.textFileStream("hdfs://scats-1-master:9000/tweets");
    tweets.count();

    ssc.sparkContext().setLogLevel("ERROR");
    ssc.start();
    ssc.awaitTermination();

  }

}
