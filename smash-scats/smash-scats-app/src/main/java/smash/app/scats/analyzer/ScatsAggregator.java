package smash.app.scats.analyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
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
import smash.data.scats.pojo.ScatsVolume;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Yikai Gong
 * Spark task for aggregate SCATS volume data by day of week.
 */

public class ScatsAggregator implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsAggregator.class);
  private SparkConf sparkConf;
  private final static int timeSlotsNum = 96;
  private final static ArrayList<String> queries = new ArrayList<>();

  public ScatsAggregator() {
    this(new SparkConf());
  }

  public ScatsAggregator(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{ScatsVolume.class};
    this.sparkConf.registerKryoClasses(classes);

    queries.add("qt_interval_count BEFORE 2017-02-01T23:59:59.999Z");
    queries.add("qt_interval_count DURING 2017-02-01T23:59:59.999Z/2017-04-01T23:59:59.999Z");
    queries.add("qt_interval_count DURING 2017-04-01T23:59:59.999Z/2017-06-01T23:59:59.999Z");
    queries.add("qt_interval_count DURING 2017-06-01T23:59:59.999Z/2017-08-01T23:59:59.999Z");
    queries.add("qt_interval_count DURING 2017-08-01T23:59:59.999Z/2017-10-01T23:59:59.999Z");
    queries.add("qt_interval_count AFTER 2017-10-01T23:59:59.999Z");
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

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, ScatsDOWFeatureFactory.createFeatureType());
    // Launch Spark session
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);

      JavaPairRDD<String, long[]> tmp_sum = JavaPairRDD.fromJavaRDD(sc.emptyRDD());
      for (String query : queries) {
        JavaPairRDD<String, long[]> scats_volume_bySite = get_scats_volume_bySite(jsp, sc, options, query);
        tmp_sum = tmp_sum.union(scats_volume_bySite).reduceByKey((a1, a2) -> {
          for (int i = 0; i < a1.length; i++) {
            a1[i] = a1[i] + a2[i];
          }
          return a1;
        });
        tmp_sum.foreachPartition(partition->{

        });
        scats_volume_bySite.unpersist();
      }

      JavaPairRDD<String, double[]> scats_avg_bySite = tmp_sum.mapToPair(tuple -> {
        double[] result = new double[2];
        result[0] = ((double) tuple._2[0]) / tuple._2[1];
        result[1] = (double) tuple._2[1];
        return new Tuple2<>(tuple._1, result);
      });

      scats_avg_bySite.persist(StorageLevel.MEMORY_AND_DISK());
      tmp_sum.unpersist();

      JavaPairRDD<String, double[]> tmp_result = JavaPairRDD.fromJavaRDD(sc.emptyRDD());
      for (String query : queries) {
        JavaPairRDD<String, long[]> scats_volume_bySite = get_scats_volume_bySite(jsp, sc, options, query);
        tmp_result = scats_volume_bySite.join(scats_avg_bySite).mapToPair(tuple -> {
          String key = tuple._1;
          long volume = tuple._2._1[0];
          double avg = tuple._2._2[0];
          double sum = tuple._2._2[1];
          double diff_square = Math.pow((avg - volume), 2);
          double[] result = new double[5];
          result[0] = avg;
          result[1] = sum;
          result[2] = diff_square;
          result[3] = volume;
          result[4] = volume;
          return new Tuple2<>(key, result);
        }).union(tmp_result)
          .reduceByKey((t1, t2) -> {
            double diff_square_sum = t1[2] + t2[2];
            t1[2] = diff_square_sum;
            if (t2[3] < t1[3])
              t1[3] = t2[3];
            if (t2[4] > t1[4])
              t1[4] = t2[4];
            return t1;
          });

        tmp_result.foreachPartition(p->{});
        scats_volume_bySite.unpersist();
      }

      JavaPairRDD<String, double[]> resultRdd = tmp_result.mapToPair(tuple->{
        double sum = tuple._2[1];
        double diff_square_sum = tuple._2[2];
        double stDevi;
        if(sum == 1){
          stDevi = 0;
        } else{
          stDevi = Math.sqrt(diff_square_sum/(sum-1));
        }
        double[] result = tuple._2;
        result[2] = stDevi;
        return new Tuple2<>(tuple._1, result);
      });
      tmp_result.unpersist();

      JavaRDD<SimpleFeature> resultRDD = resultRdd.map(tuple ->
        ScatsDOWFeatureFactory.buildFeatureFromTuple(tuple));

      Map<String, String> opt = options.getAccumuloOptions();
//      opt.put("generateStats", Boolean.FALSE.toString());
      jsp.save(resultRDD, opt, ScatsDOWFeatureFactory.FT_NAME);


//      JavaPairRDD<String, double[]> resultRdd = get_scats_volume_bySite(jsp, sc, options).join(scats_avg_bySite).mapToPair(tuple->{
//        String key = tuple._1;
//        long volume = tuple._2._1[0];
//        double avg = tuple._2._2[0];
//        double sum = tuple._2._2[1];
//        double diff_square = Math.pow((avg - volume), 2);
//        double[] result = new double[5];
//        result[0] = avg;
//        result[1] = sum;
//        result[2] = diff_square;
//        result[3] = volume;
//        result[4] = volume;
//        return new Tuple2<>(key, result);
//      }).reduceByKey((t1, t2)->{
//        double diff_square_sum = t1[2] + t2[2];
//        t1[2] = diff_square_sum;
//        if (t2[3] < t1[3])
//          t1[3] = t2[3];
//        if (t2[4] > t1[4])
//          t1[4] = t2[4];
//        return t1;
//      }).mapToPair(tuple->{
//        double sum = tuple._2[1];
//        double diff_square_sum = tuple._2[2];
//        double stDevi;
//        if(sum == 1){
//          stDevi = 0;
//        } else{
//          stDevi = Math.sqrt(diff_square_sum/(sum-1));
//        }
//        double[] result = tuple._2;
//        result[2] = stDevi;
//        return new Tuple2<>(tuple._1, result);
//      });



    }
  }

  public static JavaPairRDD<String, long[]> get_scats_volume_bySite(JavaSpatialRDDProvider jsp, JavaSparkContext sc, GeoMesaOptions options, String query_str) throws CQLException {

    Query query = new Query(ScatsFeaturePointFactory.FT_NAME, CQL.toFilter(query_str)); //, CQL.toFilter("DAY_OF_WEEK='Tue'")
    JavaRDD<SimpleFeature> scatsFeatureRDD = jsp
      .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
//      scatsFeatureRDD = scatsFeatureRDD.repartition(128);
//      scatsFeatureRDD = scatsFeatureRDD.persist(StorageLevel.MEMORY_AND_DISK());

    JavaPairRDD<String, long[]> scats_volume_bySite = scatsFeatureRDD.mapToPair(sf -> {
      ScatsVolume scatsVolume = ScatsFeaturePointFactory.fromSFtoPojo(sf);
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH");   //todo
      df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
      String key = scatsVolume.getNb_scats_site() + "#" + df.format(scatsVolume.getQt_interval_count());
      return new Tuple2<>(key, scatsVolume);
    }).reduceByKey((scv1, scv2) -> {
      scv1.setNb_detector("all");
      Integer sum = scv1.getVolume() + scv2.getVolume();
      scv1.setVolume(sum);
      return scv1;
    }).mapToPair(tuple2 -> {
      ScatsVolume scv = tuple2._2;
      String scatsSite = scv.getNb_scats_site();
      String detectorNum = scv.getNb_detector();
      Integer volume = scv.getVolume();
      String dayOfWeek = scv.getDay_of_week();
      Date date = scv.getQt_interval_count();
      SimpleDateFormat df = new SimpleDateFormat(ScatsDOWFeatureFactory.timeOfDay_exp);  //HH:mm:ss
      df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
      String timeOfDay = df.format(date);
      String geo_wkt = scv.getGeoPointString();
      String geo_wkt_line = scv.getGeoLineString();
      String key = scatsSite + "#" + detectorNum + "#" + dayOfWeek + "#" + timeOfDay + "#" + geo_wkt + "#" + geo_wkt_line;
      long[] value = new long[2];
      value[0] = volume.longValue();
      value[1] = 1l;
      return new Tuple2<>(key, value);
    });
    return scats_volume_bySite;
  }


}
