package smash.app.tweets.importer;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Yikai Gong
 */

public class TweetsStreamImporter {
//  private static Logger logger = LoggerFactory.getLogger(TweetsStreamImporter.class);


  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    TweetsImporterOptions options = new TweetsImporterOptions();
//    options.parse(args);
    TweetsStreamImporter importer = new TweetsStreamImporter();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run (TweetsImporterOptions options) throws IOException, InterruptedException {
    // Ensures Feature Type is saved in GeoMesa
//    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT);

    SparkConf conf = new SparkConf().setAppName(this.getClass().getSimpleName()).setMaster("locale[2]");
    JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
    JavaDStream<String> tweets = ssc.textFileStream("hdfs://scats-1-master:9000/tweets/");
    tweets.print();

    ssc.start();
    ssc.awaitTerminationOrTimeout(10000);

  }
}
