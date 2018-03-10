package smash.app.scats.analyzer;

import com.google.common.base.Joiner;
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
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import smash.data.scats.DateStrUtils;
import smash.data.scats.gt.ScatsDOWFeatureFactory;
import smash.data.scats.gt.ScatsFeaturePointFactory;
import smash.data.scats.pojo.ScatsVolume;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;


/**
 * @author Yikai Gong
 */

public class ScatsAbnDetector {
  private static Logger logger = LoggerFactory.getLogger(ScatsAbnDetector.class);
  private SparkConf sparkConf;

  public ScatsAbnDetector() {
    this.sparkConf = new SparkConf();
  }

  public ScatsAbnDetector(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    this.sparkConf = sparkConf;
    Class[] classes = new Class[]{ScatsVolume.class};
    this.sparkConf.registerKryoClasses(classes);
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    ScatsAbnDetector scatsAbnDetector = new ScatsAbnDetector();
    JobTimer.print(() -> {
      scatsAbnDetector.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Filter filter = CQL.toFilter("qt_interval_count during 2017-12-01T00:00:00+11:00/2017-12-31T00:59:59+11:00");
      Query query = new Query(ScatsFeaturePointFactory.FT_NAME, filter); //, CQL.toFilter("DAY_OF_WEEK='Tue'")
      JavaRDD<SimpleFeature> featureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);

      JavaPairRDD<String, ScatsVolume> pairRdd = featureRDD.mapToPair(sf -> {
        ScatsVolume scatsVolume = ScatsFeaturePointFactory.fromSFtoPojo(sf);
        String key = scatsVolume.getNb_scats_site() + "#" + scatsVolume.getQt_interval_count();
        return new Tuple2<>(key, scatsVolume);
      }).reduceByKey((scv1, scv2) -> {
        scv1.setNb_detector("all");
        Integer sum = scv1.getVolume() + scv2.getVolume();
        scv1.setVolume(sum);
        return scv1;
      }).mapToPair(pair -> {
        ScatsVolume scv = pair._2;
        String timeOfDay = DateStrUtils.getAusTimeOfDay(scv.getQt_interval_count());
        String key = Joiner.on("#").join(scv.getNb_scats_site(), scv.getDay_of_week(), timeOfDay);
        return new Tuple2<>(key, scv);
      });
      pairRdd.persist(StorageLevel.MEMORY_AND_DISK());

      Query query2 = new Query(ScatsDOWFeatureFactory.FT_NAME);
      JavaRDD<SimpleFeature> baseLineFeatureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query2);

      JavaPairRDD<String, Double> pairRdd2 = baseLineFeatureRDD.mapToPair(sf -> {
        String scats_site = (String) sf.getAttribute(ScatsDOWFeatureFactory.NB_SCATS_SITE);
        String day_of_week = (String) sf.getAttribute(ScatsDOWFeatureFactory.DAY_OF_WEEK);
        Date timeOfDay_date = (Date) sf.getAttribute(ScatsDOWFeatureFactory.TIME_OF_DAY);
        String timeOfDay = DateStrUtils.getAusTimeOfDay(timeOfDay_date);
        String key = Joiner.on("#").join(scats_site, day_of_week, timeOfDay);
        Double avg_vol = (Double) sf.getAttribute(ScatsDOWFeatureFactory.AVERAGE_VEHICLE_COUNT);
        return new Tuple2<>(key, avg_vol);
      });
      pairRdd2.persist(StorageLevel.MEMORY_AND_DISK());

      JavaPairRDD<String, Tuple2<ScatsVolume, Double>> joinedRdd = pairRdd.join(pairRdd2);

      joinedRdd = joinedRdd.filter(pair->{
        ScatsVolume scv = pair._2._1;
        Integer vol = scv.getVolume();
        Double avg_vol = pair._2._2;
        return vol > 10 * avg_vol || vol < avg_vol / 10;
      });

      joinedRdd.collect().forEach(pair->{
        ScatsVolume scv = pair._2._1;
        Integer vol = scv.getVolume();
        Double avg_vol = pair._2._2;
        String key = pair._1;
        System.out.println(key + " : " + vol + " : " + avg_vol);
      });

//      joinedRdd.flatMapToPair(pair->{
//        ArrayList<Tuple2<String, Tuple2<>>>
//        ScatsVolume scv = pair._2._1;
//        Integer vol = scv.getVolume();
//        Double avg_vol = pair._2._2;
//
//      })
    }
  }
}
