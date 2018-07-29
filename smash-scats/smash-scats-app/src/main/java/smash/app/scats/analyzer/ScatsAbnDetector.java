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
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;
import smash.app.scats.analyzer.entity.ScatsAbnEntity;
import smash.data.scats.DateStrUtils;
import smash.data.scats.gt.ScatsDOWFeatureFactory;
import smash.data.scats.gt.ScatsFeaturePointFactory;
import smash.data.scats.gt.ScatsNearByFeatureFactory;
import smash.data.scats.pojo.ScatsVolume;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.DataFormatException;


/**
 * @author Yikai Gong
 */

public class ScatsAbnDetector implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsAbnDetector.class);
  private SparkConf sparkConf;


  public ScatsAbnDetector() {
    this(new SparkConf());
  }

  public ScatsAbnDetector(SparkConf sparkConf) {
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{ScatsVolume.class};
    sparkConf.registerKryoClasses(classes);
    this.sparkConf = sparkConf;
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
//    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
//      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
//      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
//      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);

    int dayI = 1;
    int endDay = 7;
    int[] TF = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
    while (dayI <= endDay) {
      int[] TF_i = calculateOneDay(dayI, sparkConf, options);
      System.out.println(Integer.toString(dayI) + ": " + Arrays.toString(TF_i));
      for (int i = 0; i < TF.length; i++) {
        TF[i] = TF[i] + TF_i[i];
      }
      dayI++;
    }

    System.out.println("TT: " + TF[0]);
    System.out.println("TF: " + TF[1]);
    System.out.println("FT: " + TF[2]);
    System.out.println("FF: " + TF[3]);
    System.out.println("===== Tweets!=0 =====");
    System.out.println("TT: " + TF[4]);
    System.out.println("TF: " + TF[5]);
    System.out.println("FT: " + TF[6]);
    System.out.println("FF: " + TF[7]);
//    } //end try
  }

  public static int[] calculateOneDay(int DayI, SparkConf sparkConf, GeoMesaOptions options) throws CQLException {
    int[] TF = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());

      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      // Load target SCATS data
      String DayI_str = String.valueOf(DayI);
      DayI_str = "00".substring(DayI_str.length()) + DayI_str;
      Filter filter = CQL.toFilter("qt_interval_count during 2017-12-" + DayI_str + "T00:00:00+11:00/2017-12-" + DayI_str + "T23:59:59+11:00 AND BBOX(geometry, 144.895795,-37.86113,145.014087,-37.763636)");
      Query query = new Query(ScatsFeaturePointFactory.FT_NAME, filter); //, CQL.toFilter("DAY_OF_WEEK='Tue'")

      JavaPairRDD<String, ScatsVolume> pairRdd = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query)
        .repartition(12)
        .mapToPair(sf -> {
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
//    pairRdd.persist(StorageLevel.MEMORY_AND_DISK());

      // Load SCATS baseline data.
      Filter filter2 = CQL.toFilter("BBOX(geometry, 144.895795,-37.86113,145.014087,-37.763636)"); //todo remove it
      Query query2 = new Query(ScatsDOWFeatureFactory.FT_NAME, filter2);
      JavaPairRDD<String, Double[]> pairRdd2 = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query2)
//        .repartition(12)
        .mapToPair(sf -> {
          String scats_site = (String) sf.getAttribute(ScatsDOWFeatureFactory.NB_SCATS_SITE);
          String day_of_week = (String) sf.getAttribute(ScatsDOWFeatureFactory.DAY_OF_WEEK);
          Date timeOfDay_date = (Date) sf.getAttribute(ScatsDOWFeatureFactory.TIME_OF_DAY);
          String timeOfDay = DateStrUtils.getAusTimeOfDay(timeOfDay_date);
          String key = Joiner.on("#").join(scats_site, day_of_week, timeOfDay);
          Double avg_vol = (Double) sf.getAttribute(ScatsDOWFeatureFactory.AVERAGE_VEHICLE_COUNT);
          Double st_devi = (Double) sf.getAttribute(ScatsDOWFeatureFactory.STANDARD_DEVIATION);
          Double num_of_features = ((Integer) sf.getAttribute(ScatsDOWFeatureFactory.NUM_OF_FEATURES)).doubleValue();
          Double[] r = new Double[3];
          r[0] = avg_vol;
          r[1] = st_devi;
          r[2] = num_of_features;
          return new Tuple2<>(key, r);
        }).filter(pair -> {
          Double avg_vol = pair._2[0];
          Double num_of_features = pair._2[2];
          return avg_vol > 0 && num_of_features >= 10;
        });
//    pairRdd2.persist(StorageLevel.MEMORY_AND_DISK());

      // Compare target scats to baseline vie join operation
      JavaPairRDD<String, Tuple2<ScatsVolume, Double[]>> joinedRdd = pairRdd.join(pairRdd2).repartition(100);


      JavaPairRDD<String, ScatsAbnEntity> resultRdd = joinedRdd.mapToPair(pair -> {
        String key = pair._1;
        String keys[] = key.split("#");
        ScatsVolume scv = pair._2._1;
        Integer vol = scv.getVolume();
        Double avg_vol = pair._2._2[0];
        Double st_devi = pair._2._2[1];

        Date date = scv.getQt_interval_count();
        int timeDiffSec = 1800; //849  3600
        int distDiffMet = 1000;  //142
        Date date_start = DateUtils.addSeconds(date, -timeDiffSec);
        Date date_end = DateUtils.addSeconds(date, timeDiffSec);
        String wktPoint = scv.getGeoPointString();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String timePeriod = df.format(date_start) + "/" + df.format(date_end);
        String queryStr = "DWITHIN(geometry, " + wktPoint + ", " + distDiffMet + ", meters) AND created_at DURING " + timePeriod; // + " AND on_street=true";
        Query query3 = null;
        try {
          query3 = new Query(TweetsFeatureFactory.FT_NAME_OS, CQL.toFilter(queryStr));
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("date: " + df.format(date));
          System.out.println(queryStr);
        }
        GeoMesaOptions options1 = options.copy();
        options1.setTableName("tweets");
        int numOfTweets = GeoMesaDataUtils.getNumOfFeatures(options1, query3);
        int hasCore = 0;

//        Long secOfDay_start = getSecOfDay(TimeZone.getTimeZone("Australia/Melbourne"), date_start);
//        Long secOfDay_end = getSecOfDay(TimeZone.getTimeZone("Australia/Melbourne"), date_end);
//        String queryStr_baseline = "DWITHIN(geometry, " + wktPoint + ", " + distDiffMet + ", meters) AND sec_of_day > " + secOfDay_start + " AND sec_of_day < " + secOfDay_end; // + " AND on_street=true";
//        if (secOfDay_start > secOfDay_end)
//          queryStr_baseline = "DWITHIN(geometry, " + wktPoint + ", " + distDiffMet + ", meters) AND sec_of_day < " + secOfDay_start + " AND sec_of_day > " + secOfDay_end; // + " AND on_street=true";
//        Query query_baseline = new Query(TweetsFeatureFactory.FT_NAME_OS, CQL.toFilter(queryStr_baseline));
//        int not = GeoMesaDataUtils.getNumOfFeatures(options1, query_baseline);
//        double tweet_baseline = not / 200d; //200 days of tweets stored in GeoMesa

        String queryStr_baseline = ScatsNearByFeatureFactory.NB_SCATS_SITE + "=" + keys[0] + " AND " +
          ScatsNearByFeatureFactory.DAY_OF_WEEK + "=" + keys[1] + " AND " +
          ScatsNearByFeatureFactory.TIME_OF_DAY + "=" + new Integer(keys[2].split(":")[0]);
        System.out.println(queryStr_baseline);
        Query query_baseline = new Query(ScatsNearByFeatureFactory.FT_NAME, CQL.toFilter(queryStr_baseline));
        SimpleFeature tweets_baseline = GeoMesaDataUtils.getFeatures(options, query_baseline).get(0);
        Double tweets_avg = (Double) tweets_baseline.getAttribute(ScatsNearByFeatureFactory.AVERAGE);
        Double tweets_st_devi = (Double) tweets_baseline.getAttribute(ScatsNearByFeatureFactory.STANDARD_DEVIATION);

        ScatsAbnEntity entity = new ScatsAbnEntity(false, false);
        if (avg_vol > 0 && ((vol > avg_vol + st_devi) || (vol < avg_vol - st_devi)))
          entity.setScatsAbn(true);
        if (numOfTweets > tweets_avg + 2 * tweets_st_devi || numOfTweets < tweets_avg - 2 * tweets_st_devi)
          entity.setTweetsAbn(true);
        if (hasCore == 1)
          entity.setTweetCluster(true);
        if (numOfTweets == 0)
          entity.setTweetEqZero(true);

//        if(entity.getScatsAbn() && entity.getTweetsAbn()){
//          System.out.println("TT detected at: " + key + "\n" + "vol: " + vol + " avg_vol: " + avg_vol + "\n" +
//          "numOfTweets: " + numOfTweets + " avg_tweets: " + tweet_baseline + "\n" + queryStr);
//        }
        return new Tuple2<>(key, entity);
      });
//    resultRdd.persist(StorageLevel.MEMORY_AND_DISK());
      // TT TF FT FF  (SCATS-abn/TWEET-abn) + tweets!=0
      TF = resultRdd.aggregate(new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0}, (r, tuple) -> {
        ScatsAbnEntity e = tuple._2;
        if (e.getScatsAbn() && e.getTweetsAbn())
          r[0] = r[0] + 1;
        else if (e.getScatsAbn() && !e.getTweetsAbn())
          r[1] = r[1] + 1;
        else if (!e.getScatsAbn() && e.getTweetsAbn())
          r[2] = r[2] + 1;
        else if (!e.getScatsAbn() && !e.getTweetsAbn())
          r[3] = r[3] + 1;

        if (!e.getTweetEqZero()) {
          if (e.getScatsAbn() && e.getTweetsAbn())
            r[4] = r[4] + 1;
          else if (e.getScatsAbn() && !e.getTweetsAbn())
            r[5] = r[5] + 1;
          else if (!e.getScatsAbn() && e.getTweetsAbn())
            r[6] = r[6] + 1;
          else if (!e.getScatsAbn() && !e.getTweetsAbn())
            r[7] = r[7] + 1;
        }
        return r;
      }, (r1, r2) -> {
        for (int i = 0; i < r1.length; i++) {
          r1[i] = r1[i] + r2[i];
        }
        return r1;
      });
    }
    return TF;
  }

  public static Long getSecOfDay(TimeZone tz, Date date) throws ParseException {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-ddZ");
    df.setTimeZone(tz);
    Date start_of_day = df.parse(df.format(date));
    Long sec_of_day = (date.getTime() - start_of_day.getTime()) / 1000;
    return sec_of_day;
  }
}
