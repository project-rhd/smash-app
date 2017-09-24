

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import smash.stream.tweets.TweetsStreamImporter;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaOptions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Yikai Gong
 */

public class TweetsStreamImporterTest {
  private SparkConf sparkConf;

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
//    options.parse(args);
    options.password = "smash";
    options.user = "root";
    options.instanceId = "smash";
    options.tableName = "tweets2";
    options.zookeepers = "scats-1-master:2181";
    options.overwrite = true;
    SparkConf conf = new SparkConf().setMaster("local[2]");
    TweetsStreamImporter importer = new TweetsStreamImporter(conf);

    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

}
