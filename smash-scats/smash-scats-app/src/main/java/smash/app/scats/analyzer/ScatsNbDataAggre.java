package smash.app.scats.analyzer;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
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
import smash.data.scats.DateStrUtils;
import smash.data.scats.gt.ScatsDOWFeatureFactory;
import smash.data.scats.gt.ScatsFeaturePointFactory;
import smash.data.scats.gt.ScatsNearByFeatureFactory;
import smash.data.scats.pojo.ScatsVolume;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.data.tweets.pojo.Tweet;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class ScatsNbDataAggre implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsNbDataAggre.class);
  private SparkConf sparkConf;
  private static String[] day_of_weeks = new String[]{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

  public ScatsNbDataAggre() {
    this(new SparkConf());
  }

  public ScatsNbDataAggre(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{ScatsVolume.class};
    this.sparkConf.registerKryoClasses(classes);
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    ScatsNbDataAggre scatsNbDataAggre = new ScatsNbDataAggre();
    JobTimer.print(() -> {
      scatsNbDataAggre.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, ScatsNearByFeatureFactory.createFeatureType());
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Query query = new Query(ScatsDOWFeatureFactory.FT_NAME, CQL.toFilter("day_of_week='Mon'"));
      JavaRDD<SimpleFeature> scatsFeatureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);

      JavaPairRDD<String, String> scatsId_wkt = scatsFeatureRDD.mapToPair(feature -> {

        String scats_site = (String) feature.getAttribute(ScatsDOWFeatureFactory.NB_SCATS_SITE);
        String wkt = feature.getDefaultGeometry().toString();
        return new Tuple2<>(scats_site, wkt);
      }).reduceByKey((wkt1, wkt2) -> wkt1 == null ? wkt2 : wkt1).repartition(36);

      JavaPairRDD<String, double[]> key_resultRdd = scatsId_wkt.flatMapToPair(tuple -> {
        ArrayList<Tuple2<String, double[]>> resultList = new ArrayList<>();
        HashMap<String, HashMap<String, Integer>> map = initMap();
        String scats_id = tuple._1;
        String wkt = tuple._2;
        GeoMesaOptions options_t = options.copy();
        options_t.setTableName("tweets");
        int range_distance = 1000;  //1000 meters
        String query_str = "DWITHIN(geometry, " + wkt + ", " + range_distance + ", meters)";
        Query query_t = new Query(TweetsFeatureFactory.FT_NAME, CQL.toFilter(query_str));
        ArrayList<SimpleFeature> sfList = GeoMesaDataUtils.getFeatures(options_t, query_t);
        for (SimpleFeature sf : sfList) {
          String dayOfWeek = (String) sf.getAttribute(TweetsFeatureFactory.DAY_OF_WEEK);
          Date timestamp = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
          String hourOfDay = DateStrUtils.getAusHourOfDay(timestamp);
          String date_str = DateStrUtils.getAusDateStr(timestamp);
          HashMap<String, Integer> subMap = map.get(Joiner.on("#").join(dayOfWeek, hourOfDay));
          Integer x = subMap.get(date_str);
          if (x != null)
            subMap.put(date_str, x + 1);
        }
        for (Map.Entry<String, HashMap<String, Integer>> entry : map.entrySet()) {
          String key = entry.getKey();
          HashMap<String, Integer> submap = entry.getValue();
          ArrayList<Integer> list = new ArrayList<>(submap.values());
          double[] result = calculateBaseline(list);
          String newKey = Joiner.on("#").join(scats_id, wkt, key);
          resultList.add(new Tuple2<>(newKey, result));
        }
        return resultList.iterator();
      });


      JavaRDD<SimpleFeature> result_sf_rdd = key_resultRdd.map(t -> {
        if (t._2 == null)
          return null;
        return ScatsNearByFeatureFactory.buildFeatureFromTuple(t);
      });
//
      jsp.save(result_sf_rdd, options.getAccumuloOptions(), ScatsNearByFeatureFactory.FT_NAME);

//      key_rdd.collect().forEach(tuple -> {
//        System.out.println(tuple._1);
//      });
//      System.out.println("Total: " + scatsId_wkt.count());   //3723
//      System.out.println(key_rdd.count());                   //625464
//      System.out.println(result_sf_rdd.count());
//      System.out.println(result_sf_rdd);

    } // SparkSession closed
  }

  public static HashMap<String, HashMap<String, Integer>> initMap() {
    HashMap<String, HashMap<String, Integer>> map = new HashMap<>();
    for (String dayOfWeek : day_of_weeks) {
      for (int i = 0; i < 24; i++) {
        String hour_str = String.valueOf(i);
        hour_str = "00".substring(hour_str.length()) + hour_str + ":00:00";
        String key = Joiner.on("#").join(dayOfWeek, hour_str);
        HashMap<String, Integer> subMap = new HashMap<>();
        List<String> dates = getDatesFromDayOfWeek(dayOfWeek);
        for (String date_str : dates) {
          subMap.put(date_str, 0);
        }
        map.put(key, subMap);
      }
    }
    return map;
  }

  public static double[] calculateBaseline(ArrayList<Integer> list) {
    if (list.size() < 2)
      return null;
    int sum = 0;
    int max = 0;
    int min = 0;
    for (Integer tweets : list) {
      sum = sum + tweets;
      max = tweets > max ? tweets : max;
      min = tweets < min ? tweets : min;
    }
    double avg = sum * 1.0d / list.size();
    double diff_square_sum = 0d;
    for (Integer tweets : list) {
      diff_square_sum = diff_square_sum + Math.pow((avg - tweets), 2);
    }
    double st_devi = Math.sqrt(diff_square_sum / (list.size() - 1));
    return new double[]{avg, (double) list.size(), st_devi, (double) min, (double) max};
  }

  public static ArrayList<String> getDatesFromDayOfWeek(String day_of_week) {
    ArrayList<String> dates = new ArrayList<>();
    LocalDate start = LocalDate.of(2017, 6, 1);
    LocalDate stop = LocalDate.of(2018, 1, 1);
    DayOfWeek dayOfWeek = null;
    switch (day_of_week.toLowerCase()) {
      case "mon":
        dayOfWeek = DayOfWeek.MONDAY;
        break;
      case "tue":
        dayOfWeek = DayOfWeek.TUESDAY;
        break;
      case "wed":
        dayOfWeek = DayOfWeek.WEDNESDAY;
        break;
      case "thu":
        dayOfWeek = DayOfWeek.THURSDAY;
        break;
      case "fri":
        dayOfWeek = DayOfWeek.FRIDAY;
        break;
      case "sat":
        dayOfWeek = DayOfWeek.SATURDAY;
        break;
      case "sun":
        dayOfWeek = DayOfWeek.SUNDAY;
        break;
    }
    LocalDate date = start.with(TemporalAdjusters.nextOrSame(dayOfWeek));
    while (date.isBefore(stop)) {
      dates.add(date.toString());
      // Set up the next loop.
      date = date.plusWeeks(1);
    }
    return dates;
  }


}
