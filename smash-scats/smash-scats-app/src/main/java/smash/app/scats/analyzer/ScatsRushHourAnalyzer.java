package smash.app.scats.analyzer;

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
import smash.data.scats.gt.ScatsDOWFeatureFactory;
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

public class ScatsRushHourAnalyzer implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsRushHourAnalyzer.class);
  private SparkConf sparkConf;

  public static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
  public static final String FORMAT_STR = "yyyy-MM-dd'T'HH zzz";
  public static final TimeZone mel = TimeZone.getTimeZone("Australia/Melbourne");
  public static final TimeZone utc = TimeZone.getTimeZone("UTC");
  public static final SimpleDateFormat isoFormatter = new SimpleDateFormat(FORMAT_STR);

  static {
    isoFormatter.setTimeZone(utc);
  }

  public ScatsRushHourAnalyzer() {
    this(new SparkConf());
  }

  public ScatsRushHourAnalyzer(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    ScatsRushHourAnalyzer scatsRushHourAnalyzer = new ScatsRushHourAnalyzer();
    JobTimer.print(() -> {
      scatsRushHourAnalyzer.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Filter filter = CQL.toFilter("day_of_week='Mon'");
      Query query = new Query(ScatsDOWFeatureFactory.FT_NAME, filter);
      JavaRDD<SimpleFeature> featureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
      JavaPairRDD<String, Double> pairRDD = featureRDD.mapToPair(sf -> {
        Date time = (Date) sf.getAttribute(ScatsDOWFeatureFactory.TIME_OF_DAY);
        Double avgCount = (Double) sf.getAttribute(ScatsDOWFeatureFactory.AVERAGE_VEHICLE_COUNT);
        String timeStr = isoFormatter.format(time);
        return new Tuple2<>(timeStr, avgCount);
      }).reduceByKey((count1, count2) -> count1 + count2);

      Map<String, Double> time_count_map = pairRDD.collectAsMap();
      time_count_map.forEach((time, count)->
        System.out.println(time + " : " + count));
    }
  }
}
