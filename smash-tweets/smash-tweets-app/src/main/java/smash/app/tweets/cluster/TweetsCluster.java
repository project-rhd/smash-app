package smash.app.tweets.cluster;


//import com.vividsolutions.jts.geom.Geometry;
//import com.vividsolutions.jts.geom.Point;
//import dbis.stark.STObject;
//import dbis.stark.spatial.SpatialRDD;
//import dbis.stark.spatial.SpatialRDDFunctions;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.SparkSession;
//import org.geotools.data.Query;
//import org.geotools.factory.Hints;
//import org.geotools.filter.text.cql2.CQL;
//import org.geotools.filter.text.cql2.CQLException;
//import org.geotools.geometry.jts.ReferencedEnvelope;
//import org.kohsuke.args4j.CmdLineException;
//import org.locationtech.geomesa.spark.GeoMesaSpark;
//import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
//import org.locationtech.geomesa.spark.SpatialRDDProvider;
//import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
//import org.opengis.feature.simple.SimpleFeature;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.Tuple2;
//import scala.reflect.ClassTag;
//import smash.data.tweets.gt.SpatialClusterFactory;
//import smash.data.tweets.gt.TweetsFeatureFactory;
//import smash.utils.JobTimer;
//import smash.utils.geomesa.GeoMesaDataUtils;
//
//import java.io.IOException;
import java.io.Serializable;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;

/**
 * @author Yikai Gong
 */

public class TweetsCluster implements Serializable {
//  private static Logger logger = LoggerFactory.getLogger(TweetsCluster.class);
//
//  public static void main(String[] args)
//    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
//    TweetsClusterOptions options = new TweetsClusterOptions();
//    options.parse(args);
//    TweetsCluster cluster = new TweetsCluster();
//    JobTimer.print(() -> {
//      cluster.run(options);
//      return null;
//    });
//    System.exit(0);
//  }
//
//  private SparkConf sparkConf;
//
//  private TweetsCluster() {
//    this(new SparkConf());
//  }
//
//  private TweetsCluster(SparkConf sparkConf) {
//    this.sparkConf = sparkConf;
//    sparkConf.setAppName(this.getClass().getSimpleName());
////    sparkConf.set("spark.files.maxPartitionBytes", "33554432"); // 32MB
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//    // ** Important: GeoMesaSparkKryoRegistrator implementing serialize and de-serialize of SimpleFeature Object
//    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
////    Class[] classes = new Class[]{NPoint.class, NPoint$.class, NRectRange$.class, NRectRange.class};
////    sparkConf.registerKryoClasses(classes);
//    sparkConf.set("spark.kryoserializer.buffer.max", "128m");
//    sparkConf.set("spark.network.timeout", "600s");
//    sparkConf.set("spark.driver.maxResultSize", "2g");
////    sparkConf.set("spark.sql.crossJoin.enabled","true");
//  }
//
//  private void run(TweetsClusterOptions options) throws IOException, CQLException {
//    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
//      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
//      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
//      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
//      //=== Task 1: Run MR-DBSCAN and save data with cluster flag
//      Query query = new Query(TweetsFeatureFactory.FT_NAME);
//      JavaRDD<SimpleFeature> tweetsRDD = jsp
//        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
//      ReferencedEnvelope envelope = GeoMesaDataUtils.getBoundingBox(options, query);
//      int numOfExecutors = sc.sc().getExecutorIds().size();
//      System.out.println("Num of Partition: " + tweetsRDD.getNumPartitions());
//      System.out.println("Number of executors: " + numOfExecutors);
//      tweetsRDD = tweetsRDD.repartition(numOfExecutors * 2);
//      System.out.println("Num of Partition (After Repartition): " + tweetsRDD.getNumPartitions());
//      Map<String, Object> envMap = new HashMap<>();
//      envMap.put("minX", envelope.getMinX());
//      envMap.put("minY", envelope.getMinY());
//      envMap.put("maxX", envelope.getMaxX());
//      envMap.put("maxY", envelope.getMaxY());
//
//      JavaPairRDD<STObject, SimpleFeature> pairRDD = tweetsRDD.mapToPair(sf -> {
//        Geometry geom = (Geometry) sf.getDefaultGeometry();
//        Date date = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
//        STObject stObject = STObject.apply(geom, date.getTime());
//        return new Tuple2<>(stObject, sf);
//      });
//      ClassTag<STObject> SOTag = scala.reflect.ClassTag$.MODULE$.apply(STObject.class);
//      ClassTag<SimpleFeature> SFTag = scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class);
//      SpatialRDDFunctions<STObject, SimpleFeature> spatialRDD = SpatialRDD.convertSpatialPlain(pairRDD.rdd(), SOTag, SFTag);
//      // TODO auto calculate these parameters
//      double kms_per_radian_mel = 87.944d; //6371d;
//      double range = 0.1;  // km
//      long duration = 10 * 60 * 1000; // milli sec
//      double epsilon = calculate_epsilon(range, duration, kms_per_radian_mel);
//      double speed = calculate_speed(range, duration, kms_per_radian_mel);
//      double qGridCellSize = calculate_epsilon(0.5, 0, kms_per_radian_mel) * 2;   //0.5
//      int maxPointsPerCell = 100;
//      System.out.println("Range = " + range + " km");
//      System.out.println("Duration = " + duration + " milliseconds sec");
//      System.out.println("Epsilon = " + epsilon + " degree");
//      System.out.println("QGridCellSize = " + qGridCellSize + " degree");
//      System.out.println("Max num of Points per cell = " + maxPointsPerCell);
//      JavaRDD<Tuple2<String, Tuple2<Object, SimpleFeature>>> results =
//        spatialRDD
//          .cluster(3, epsilon, speed, qGridCellSize, new SerializableFunction1<Tuple2<STObject, SimpleFeature>, String>() {
//            @Override
//            public String apply(Tuple2<STObject, SimpleFeature> v1) {
//              return v1._2().getID();
//            }
//          }, false, maxPointsPerCell, scala.Option.apply(null), envMap)  //envMap
//          .toJavaRDD();
//
//      JavaRDD<SimpleFeature> resultsRDD = results.map(t -> {
//        Integer clusterId = (Integer) t._2()._1();
//        String clusterLabel = t._1();
//        SimpleFeature sf = t._2()._2();
//        sf.setAttribute(TweetsFeatureFactory.CLUSTER_ID, clusterId);
//        sf.setAttribute(TweetsFeatureFactory.CLUSTER_LABEL, clusterLabel);
//        sf.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
////        System.out.println(sf.getID());
//        return sf;
//      });
//      jsp.save(resultsRDD, options.getAccumuloOptions(), TweetsFeatureFactory.FT_NAME);
//
//
//      //=== Task 2: Retrieve clustered data and save geo-graphic centre point of each cluster
//      GeoMesaDataUtils.saveFeatureType(options, SpatialClusterFactory.SFT);
//      query.setFilter(CQL.toFilter(TweetsFeatureFactory.CLUSTER_ID + " is not null"));
//      JavaRDD<SimpleFeature> tweetsInCluster = jsp
//        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
//
//      JavaRDD<SimpleFeature> clusterSFRDD = tweetsInCluster.mapToPair(sf -> {
//        Integer clusterId = (Integer) sf.getAttribute(TweetsFeatureFactory.CLUSTER_ID);
//        Date createdAt = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
//        Point p = (Point) sf.getDefaultGeometry();
//        Double lon = p.getX();
//        Double lat = p.getY();
//        String[] parameters = {lon.toString(), lat.toString(), Long.toString(createdAt.getTime()), "1"};
//        return new Tuple2<>(clusterId, parameters);
//      }).reduceByKey((param1, param2) -> {
//        Double lonSum = Double.parseDouble(param1[0]) + Double.parseDouble(param2[0]);
//        Double latSum = Double.parseDouble(param1[1]) + Double.parseDouble(param2[1]);
//        Long timeSum = Long.parseLong(param1[2]) + Long.parseLong(param2[2]);
//        Integer count = Integer.parseInt(param1[3]) + Integer.parseInt(param2[3]);
//        String[] paramSum = {lonSum.toString(), latSum.toString(), timeSum.toString(), count.toString()};
//        return paramSum;
//      }).map(t -> {
//        Integer clusterId = t._1();
//        String[] param = t._2();
//        Integer count = Integer.parseInt(param[3]);
//        Double lon = Double.parseDouble(param[0]) / count;
//        Double lat = Double.parseDouble(param[1]) / count;
//        Long time = Long.parseLong(param[2]) / count;
//        return SpatialClusterFactory.createFeature(clusterId, lon, lat, time, count);
//      });
//      jsp.save(clusterSFRDD, options.getAccumuloOptions(), SpatialClusterFactory.FT_NAME);
//
//
//    }
//  }

  public static double calculate_epsilon(double km, long milSec, double kmsPerRadian) {
    double d1 = km / kmsPerRadian;
//    double kmsPerSecond = km / milSec;
//    double d2 = milSec * kmsPerSecond / kmsPerRadian;
    double d2 = milSec == 0d ? 0 : d1;
    double d = d1 * d1 + d2 * d2;
    return Math.sqrt(d);
  }

  public static double calculate_speed(double km, long milSec, double kmsPerRadian) {
    double d = km / kmsPerRadian;
    return d / milSec;
  }

}
