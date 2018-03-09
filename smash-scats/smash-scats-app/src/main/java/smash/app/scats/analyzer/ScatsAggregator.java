package smash.app.scats.analyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.geotools.data.Query;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import smash.data.scats.gt.ScatsDOWFeatureFactory;
import smash.data.scats.gt.ScatsFeaturePointFactory;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * @author Yikai Gong
 * Spark task for aggregate SCATS volume data by day of week.
 */

public class ScatsAggregator implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsAggregator.class);
  private SparkConf sparkConf;
  private final static int timeSlotsNum = 96;

  public ScatsAggregator() {
    this(new SparkConf());
  }

  public ScatsAggregator(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    ScatsAggregator dataAnalyzer = new ScatsAggregator();
    JobTimer.print(() -> {
      dataAnalyzer.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, ScatsDOWFeatureFactory.createFeatureType());
    // Launch Spark session
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Query query = new Query(ScatsFeaturePointFactory.FT_NAME); //, CQL.toFilter("DAY_OF_WEEK='Tue'")

      JavaRDD<SimpleFeature> scatsFeatureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
      scatsFeatureRDD = scatsFeatureRDD.repartition(128);
      scatsFeatureRDD = scatsFeatureRDD.persist(StorageLevel.MEMORY_AND_DISK());
      JavaPairRDD<String, double[]> rddResult = scatsFeatureRDD.mapToPair(simpleFeature -> {
        String scatsSite = (String) simpleFeature.getAttribute(ScatsFeaturePointFactory.NB_SCATS_SITE);
        String detectorNum = (String) simpleFeature.getAttribute("NB_DETECTOR");
        Integer volume = (Integer) simpleFeature.getAttribute(ScatsFeaturePointFactory.VOLUME);
        String dayOfWeek = (String) simpleFeature.getAttribute(ScatsFeaturePointFactory.DAY_OF_WEEK);
        Date date = (Date) simpleFeature.getAttribute(ScatsFeaturePointFactory.QT_INTERVAL_COUNT);
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
        String timeOfDay = df.format(date);
        String geo_wkt = (simpleFeature.getDefaultGeometry()).toString();
        Object geo_line = simpleFeature.getAttribute(ScatsFeaturePointFactory.UNIQUE_ROAD);
        String geo_wkt_line = null;
        if (geo_line != null)
          geo_wkt_line = geo_line.toString();
        detectorNum = "all";
//        dayOfWeek = "all";
        String key = scatsSite + "#" + detectorNum + "#" + dayOfWeek + "#" + timeOfDay + "#" + geo_wkt + "#" + geo_wkt_line;

        long[] value = new long[2];
        value[0] = volume.longValue();
        value[1] = 1l;
        return new Tuple2<>(key, value);
      }).reduceByKey((a1, a2) -> {
        for (int i = 0; i < a1.length; i++) {
          a1[i] = a1[i] + a2[i];
        }
        return a1;
      }).mapToPair(tuple -> {
        double[] result = new double[2];
        for (int i = 0; i < result.length; i++) {
          result[i] = tuple._2[i] / (double) tuple._2[tuple._2.length - 1];
        }
        result[result.length - 1] = (double) tuple._2[tuple._2.length - 1];
        return new Tuple2<>(tuple._1, result);
      });

      JavaRDD<SimpleFeature> resultRDD = rddResult.map(tuple ->
        ScatsDOWFeatureFactory.buildFeatureFromTuple(tuple));

      Map<String, String> opt = options.getAccumuloOptions();
//      opt.put("generateStats", Boolean.FALSE.toString());
      jsp.save(resultRDD, opt, ScatsDOWFeatureFactory.FT_NAME);
    }
  }


}
