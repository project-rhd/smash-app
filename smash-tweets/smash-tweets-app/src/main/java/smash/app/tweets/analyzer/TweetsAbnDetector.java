package smash.app.tweets.analyzer;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Strings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
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
import smash.data.tweets.pojo.Tweet;
import smash.data.tweets.pojo.TweetCoordinates;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Yikai Gong
 */

public class TweetsAbnDetector implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(TweetsAbnDetector.class);
  private SparkConf sparkConf;

  @Option(name = "--distanceRange", required = false, usage = "range in searching nearby Scats Entities")
  public int distanceRange = 500;

  public TweetsAbnDetector() {
    this(new SparkConf());
  }

  public TweetsAbnDetector(SparkConf sparkConf) {
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{ScatsVolume.class, Tweet.class, TweetCoordinates.class, ArrayList.class, AbnCounter.class};
    sparkConf.registerKryoClasses(classes);
    this.sparkConf = sparkConf;
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);


    TweetsAbnDetector tweetsAbnDetector = new TweetsAbnDetector();
    JobTimer.print(() -> {
      tweetsAbnDetector.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      // Load target Tweets data
      Filter filter = CQL.toFilter("created_at during 2017-06-01T00:00:00+11:00/2017-12-31T23:59:59+11:00 and cluster_id is not null");
      Query query = new Query(TweetsFeatureFactory.FT_NAME, filter); //, CQL.toFilter("DAY_OF_WEEK='Tue'")

      JavaPairRDD<String, SimpleFeature> cid_t_pairRdd = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query)
        .repartition(12)
        .mapToPair((sf -> {
          String c_id = (String) sf.getAttribute(TweetsFeatureFactory.CLUSTER_ID);
          return new Tuple2<>(c_id, sf);
        }))
        .filter(pair -> !Strings.isNullOrEmpty(pair._1));

      JavaPairRDD<String, Tuple2<double[], long[]>> cid_bbx_pair = cid_t_pairRdd.aggregateByKey(new Tuple2<double[], long[]>(new double[]{0, 0, 0}, new long[]{-1, -1}), (tup, sf) -> {
        Point point = (Point) sf.getDefaultGeometry();
        Date date = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
        double[] geoAry = tup._1;
        long[] dateAry = tup._2;
        geoAry[0] = geoAry[0] + point.getX();
        geoAry[1] = geoAry[1] + point.getY();
        geoAry[2] = geoAry[2] + 1;

        dateAry[0] = dateAry[0] < 0 || date.getTime() < dateAry[0] ? date.getTime() : dateAry[0];
        dateAry[1] = dateAry[1] < 0 || date.getTime() > dateAry[1] ? date.getTime() : dateAry[1];
        return new Tuple2<>(geoAry, dateAry);
      }, (a1, a2) -> {
        double[] geoAry1 = a1._1;
        long[] dateAry1 = a1._2;
        double[] geoAry2 = a2._1;
        long[] dateAry2 = a2._2;
        geoAry1[0] = geoAry1[0] + geoAry2[0];
        geoAry1[1] = geoAry1[1] + geoAry2[1];
        geoAry1[2] = geoAry1[2] + geoAry2[2];

        dateAry1[0] = dateAry1[0] < dateAry2[0] ? dateAry1[0] : dateAry2[0];
        dateAry1[1] = dateAry1[1] > dateAry2[1] ? dateAry1[1] : dateAry2[1];
        return new Tuple2<>(geoAry1, dateAry1);
      }).mapToPair(pair -> {
        double[] geoAry = pair._2._1;
        long[] dateAry = pair._2._2;

        double avg_lon = geoAry[0] / geoAry[2];
        double avg_lat = geoAry[1] / geoAry[2];
        long min_date = dateAry[0];
        long max_date = dateAry[1];
        return new Tuple2<>(pair._1, new Tuple2<>(new double[]{avg_lon, avg_lat}, new long[]{min_date, max_date}));
      });

      JavaPairRDD<String, Tuple2<ScatsVolume, String>> key_scv_cid_rdd = cid_bbx_pair.mapToPair(pair -> {
        String clusterId = pair._1;
        double[] geoAry = pair._2._1;
        long[] dateAry = pair._2._2;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        String periodStr = df.format(new Date(dateAry[0] - 1000)) + "/" + df.format(new Date(dateAry[1] + 1000));

        String point_wkt = "POINT (" + geoAry[0] + " " + geoAry[1] + ")";
        String queryStr_1 = "qt_interval_count during " + periodStr + " AND  DWITHIN(geometry, " + point_wkt + ", " + this.distanceRange + ", meters)";
        Filter filter1 = CQL.toFilter(queryStr_1);
//        System.out.println("qt_interval_count during " + periodStr + " AND  DWITHIN(geometry, " + point_wkt + ", " + distDiffMet + ", meters)");
        Query query1 = new Query(ScatsFeaturePointFactory.FT_NAME, filter1);
        GeoMesaOptions options1 = options.copy();
        options1.setTableName("scats_2017");
        ArrayList<SimpleFeature> sfList = GeoMesaDataUtils.getFeatures(options1, query1);
        return new Tuple2<>(clusterId+"#"+queryStr_1, sfList); //fixme
      }).flatMapToPair(pair -> {
        ArrayList<Tuple2<String, Tuple2<ScatsVolume, String>>> rList = new ArrayList<>();
        String clusterId = pair._1;
        ArrayList<SimpleFeature> sfList = pair._2;
        for (SimpleFeature sf : sfList) {
          ScatsVolume scatsVolume = ScatsFeaturePointFactory.fromSFtoPojo(sf);
          String scats_key = scatsVolume.getNb_scats_site() + "#" + scatsVolume.getQt_interval_count();
          rList.add(new Tuple2<>(scats_key, new Tuple2<>(scatsVolume, clusterId)));
        }
        return rList.iterator();
      }).reduceByKey((t1, t2) -> {
        ScatsVolume scv1 = t1._1;
        ScatsVolume scv2 = t2._1;
        Integer sum = scv1.getVolume() + scv2.getVolume();
        scv1.setNb_detector("all");
        scv1.setVolume(sum);
        return new Tuple2<>(scv1, t1._2);
      }).mapToPair(pair -> {
        ScatsVolume scv = pair._2._1;
        String clusterId = pair._2._2;
        String timeOfDay = DateStrUtils.getAusTimeOfDay(scv.getQt_interval_count());
        String key = Joiner.on("#").join(scv.getNb_scats_site(), scv.getDay_of_week(), timeOfDay);
        return new Tuple2<>(key, new Tuple2<>(scv, clusterId));
      });

      Filter filter2 = CQL.toFilter("num_of_features > 30 and average_vehicle_count>0");
      GeoMesaOptions options_scats = options.copy();
      options_scats.setTableName("scats_2017");
      Query query2 = new Query("ScatsDayOfWeekBySite", filter2);
      JavaPairRDD<String, Double[]> key_avgVol_pair = jsp
        .rdd(new Configuration(), sc, options_scats.getAccumuloOptions(), query2)
        .mapToPair(sf -> {
          String day_of_week = (String) sf.getAttribute(ScatsDOWFeatureFactory.DAY_OF_WEEK);
          String scats_site = (String) sf.getAttribute(ScatsDOWFeatureFactory.NB_SCATS_SITE);
          Date timeOfDay_date = (Date) sf.getAttribute(ScatsDOWFeatureFactory.TIME_OF_DAY);
          String timeOfDay = DateStrUtils.getAusTimeOfDay(timeOfDay_date);
          String key = Joiner.on("#").join(scats_site, day_of_week, timeOfDay);
          Double avg_vol = (Double) sf.getAttribute(ScatsDOWFeatureFactory.AVERAGE_VEHICLE_COUNT);
          Double st_devi = (Double) sf.getAttribute(ScatsDOWFeatureFactory.STANDARD_DEVIATION);
          Double[] r = new Double[]{avg_vol, st_devi};
          return new Tuple2<>(key, r);
        });

      // Merge area volumes before comparison
      JavaPairRDD<String, AbnCounter> areaRdd = key_scv_cid_rdd.join(key_avgVol_pair).mapToPair(pair -> {
        ScatsVolume scv = pair._2._1._1;
        Integer vol = scv.getVolume();
        String clusterId = pair._2._1._2;
        Double avg_vol = pair._2._2[0];
        Double st_devi = pair._2._2[1];
        Boolean abn = false;

        if (vol > (avg_vol + 2 * st_devi) || (vol < avg_vol - 2 * st_devi)) {
          if(clusterId.split("#")[0].equals("942afc93-0fab-4e92-abd0-6ceed6510ea3")){
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
            System.out.println("site: " +scv.getNb_scats_site() + ", timestamp: " + df.format(scv.getQt_interval_count()) + ", dayOfWeek: " + scv.getDay_of_week()
              + ", avg: " + avg_vol + ", st_devi: " + st_devi + ", vol: " + vol);
          }
          abn = true;
        }
        return new Tuple2<>(clusterId, new Tuple2<>(scv, abn));
      }).aggregateByKey(new AbnCounter(), (counter, tuple) -> {
        Boolean isAbn = tuple._2;
        if (isAbn)
          counter.addAbnScv(tuple._1);
        else
          counter.addNormScv(tuple._1);
        return counter;
      }, AbnCounter::merge);


      JavaPairRDD<String, int[]> cid_resultRdd = areaRdd.mapToPair(tuple -> {
        String cid = tuple._1;
        Integer numOfAbn = tuple._2.getAbnList().size();
        Integer numOfNorm = tuple._2.getNormList().size();
        return new Tuple2<>(cid, new int[]{numOfAbn, numOfNorm});
      });

      System.out.println("ClusterID : num of abn SCATS entities : num of normal SCATS entities : isAbn?");
      List<Tuple2<String, int[]>> resultList = cid_resultRdd.collect();
      int total = 0;
      int tt = 0;
      int tf = 0;

      for (Tuple2<String, int[]> tuple : resultList) {
        boolean abn = false;
        if (tuple._2[0] > 0) {
          abn = true;
          tt++;
        } else {
          tf++;
        }
        System.out.println(tuple._1.split("#")[0] + " : " + tuple._2[0] + " : " + tuple._2[1] + " : " + abn +"\n" + tuple._1.split("#")[1]);

        total++;
      }
      System.out.println("Total Clusters: " + total);
      System.out.println("TT clusters: " + tt);
      System.out.println("TF clusters: " + tf);
      double rate = tt * 100.0 / total;
      System.out.println("TT/(TT+TF): " + rate + "%");


//        .mapToPair(pair->{
//        String clusterId = pair._2._2;
//        ScatsVolume scv = pair._2._1;
//        return new Tuple2<>(clusterId, scv);
//      }).aggregateByKey(new ArrayList<ScatsVolume>(), (ary, scv)->{
//        ary.add(scv);
//        return  ary;
//      }, (ary1, ary2)->{
//        ary1.addAll(ary2);
//        return ary1;
//      }).mapToPair(cid_scvList ->{
//        int t = 0;
//        int f = 0;
//        String clusterId = cid_scvList._1;
//        ArrayList<ScatsVolume> scvList = cid_scvList._2;
//        GeoMesaOptions options1 = options.copy();
//        options1.setTableName("scats_2017");
//        for(ScatsVolume scv : scvList){
//          String scats_site = scv.getNb_scats_site();
//          String day_of_week = scv.getDay_of_week();
//          String timeOfDay = DateStrUtils.getAusTimeOfDay(scv.getQt_interval_count());
//          Filter filter1 = CQL.toFilter("nb_scats_site='"+ scats_site +"' AND day_of_week='"+ day_of_week +"' AND ");
//          Query query1 = new Query(ScatsDOWFeatureFactory.FT_NAME,filter1);
//          GeoMesaDataUtils.getFeatures(options1, null);
//        }
//        return null;
//      });


//
//      System.out.println(key_vol_cid_pair.count());
//      pairRdd.foreach(pair->{
//        System.out.println(pair._1 + " : " + pair._2.size());
//      });


//      System.out.println("Total tweets: " + pairRdd.count());
//      Map<String, Long> r = pairRdd.countByKey();
//      System.out.println("Total clusters: " + r.size());
//      System.out.println("ClusterId : numOfTweets");
//      r.forEach((k,v)->{
//        System.out.println(k + " : " + v);
//      });
    }
  }
}
