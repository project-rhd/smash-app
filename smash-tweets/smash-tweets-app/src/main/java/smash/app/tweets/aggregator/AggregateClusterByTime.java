package smash.app.tweets.aggregator;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author Yikai Gong
 */

public class AggregateClusterByTime implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(AggregateClusterByTime.class);
  private SparkConf sparkConf;

  public static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
  public static final String FORMAT_STR = "HH zzz";
  public static final TimeZone mel = TimeZone.getTimeZone("Australia/Melbourne");
  public static final TimeZone utc = TimeZone.getTimeZone("UTC");
  public static final SimpleDateFormat isoFormatter = new SimpleDateFormat(FORMAT_STR);

  static {
    isoFormatter.setTimeZone(mel);
  }

  public AggregateClusterByTime() {
    this(new SparkConf());
  }

  public AggregateClusterByTime(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    AggregateClusterByTime aggregateClusterByTime = new AggregateClusterByTime();
    JobTimer.print(() -> {
      aggregateClusterByTime.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Filter filter = CQL.toFilter(TweetsFeatureFactory.CLUSTER_ID + " is not null");
      Query query = new Query(TweetsFeatureFactory.FT_NAME, filter);
      JavaRDD<SimpleFeature> featureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
      JavaPairRDD<String, Long> pairRDD = featureRDD.mapToPair(sf -> {
        Date time = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
        String timeStr = isoFormatter.format(time);
        return new Tuple2<>(timeStr, 1L);
      }).reduceByKey((i, j) -> i + j);

      Map<String, Long> time_count_map = pairRDD.collectAsMap();
      time_count_map.forEach((time, count)->
        System.out.println(time + " : " + count));
    }
  }
}
