package smash.app.scats.analyzer;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.time.DateUtils;
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
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
import java.util.zip.DataFormatException;


/**
 * @author Yikai Gong
 */

public class ScatsAbnDetector {
  private static Logger logger = LoggerFactory.getLogger(ScatsAbnDetector.class);
  private SparkConf sparkConf;
  private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

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

      // Load target SCATS data
      Filter filter = CQL.toFilter("qt_interval_count during 2017-12-01T00:00:00+11:00/2017-12-31T00:59:59+11:00 AND BBOX(geometry, 144.895795,-37.86113,145.014087,-37.763636)");
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

      // Load SCATS baseline data.
      Filter filter2 = CQL.toFilter("BBOX(geometry, 144.895795,-37.86113,145.014087,-37.763636)");
      Query query2 = new Query(ScatsDOWFeatureFactory.FT_NAME, filter2);
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

      // Compare target scats to baseline vie join operation
      JavaPairRDD<String, Tuple2<ScatsVolume, Double>> joinedRdd = pairRdd.join(pairRdd2);
      // Filter out abnormal s=scats record
      joinedRdd = joinedRdd.filter(pair -> {
        ScatsVolume scv = pair._2._1;
        Integer vol = scv.getVolume();
        Double avg_vol = pair._2._2;
        return avg_vol > 0 && (vol > 10 * avg_vol);   //|| vol < avg_vol / 10
      });

      JavaPairRDD<String, Tuple2<ScatsVolume, double[]>> resultRdd = joinedRdd.mapToPair(pair -> {
        String key = pair._1;
        ScatsVolume scv = pair._2._1;
        Integer vol = scv.getVolume();
        Double avg_vol = pair._2._2;
        Date date = scv.getQt_interval_count();
        int timeDiffSec = 3600; //849
        int distDiffMet = 1000;  //142
        Date date_start = DateUtils.addSeconds(date, -timeDiffSec);
        Date date_end = DateUtils.addSeconds(date, timeDiffSec);

        String wktPoint = scv.getGeoPointString();
        String timePeriod = df.format(date_start) + "/" + df.format(date_end);
        String queryStr = "DWITHIN(geometry, " + wktPoint + ", " + distDiffMet + ", meters) AND created_at DURING " + timePeriod;

        Query query3 = new Query(TweetsFeatureFactory.FT_NAME, CQL.toFilter(queryStr));

        GeoMesaOptions options1 = options.copy();
        options1.setTableName("tweets");

//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
//        df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
//        System.out.println(scv.toString(df));
//        System.out.println(queryStr);
        ArrayList<SimpleFeature> features = GeoMesaDataUtils.getFeatures(options1, query3);
        int numOfTweets = features.size();
        int hasCore = 0;
        for (SimpleFeature sf : features) {
          if (sf.getAttribute(TweetsFeatureFactory.CLUSTER_ID) != null)
            hasCore = 1;
        }


//        System.out.println(numOfTweets);


        double[] result = {(double) vol, avg_vol, (double) numOfTweets, (double) hasCore};
        return new Tuple2<>(key, new Tuple2<>(scv, result));
      });

      resultRdd.collect().forEach(pair -> {
        ScatsVolume scv = pair._2._1;
        double[] result = pair._2._2;
        double vol = result[0];
        double avg_vol = result[1];
        double numOfTweets = result[2];
        boolean hasCore = result[3] == 1d;
        String key = pair._1;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
        df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
        if (numOfTweets > 0d)
          System.out.println(scv.toString(df) + " : " + vol + " : " + avg_vol + " : " + numOfTweets + " : " + hasCore);
      });
      System.out.println("Total results: " + resultRdd.count());

//      // Testing code
//      joinedRdd.takeSample(false, 100).forEach(pair->{
//        ScatsVolume scv = pair._2._1;
//        Integer vol = scv.getVolume();
//        Double avg_vol = pair._2._2;
//        String key = pair._1;
//        System.out.println(key + " : " + vol + " : " + avg_vol);
//      });
//      System.out.println("Total results: " +  joinedRdd.count());


    }
  }
}
