

import org.apache.spark.SparkConf;
import org.kohsuke.args4j.CmdLineException;
import smash.stream.tweets.StreamAppOptions;
import smash.stream.tweets.TweetsStreamCluster;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaOptions;

/**
 * @author Yikai Gong
 */

public class TweetsStreamClusterTest {
  private SparkConf sparkConf;

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    StreamAppOptions options = new StreamAppOptions();
//    options.parse(args);
    options.password = "smash";
    options.user = "root";
    options.instanceId = "smash";
    options.tableName = "tweets2";
    options.zookeepers = "scats-1-master:2181";
    options.overwrite = true;
    options.intervalSec = 20l;
    options.maxPts=100l;
    SparkConf conf = new SparkConf().setMaster("local[2]");
    TweetsStreamCluster importer = new TweetsStreamCluster(conf);

    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

}
