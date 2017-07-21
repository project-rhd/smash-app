package smash.app.tweets.cluster;


import com.vividsolutions.jts.geom.Geometry;
import dbis.stark.STObject;
import dbis.stark.spatial.SpatialRDD;
import dbis.stark.spatial.SpatialRDDFunctions;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.Query;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.utils.JobTimer;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Yikai Gong
 */

public class TweetsCluster implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(TweetsCluster.class);

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    TweetsClusterOptions options = new TweetsClusterOptions();
    options.parse(args);
    TweetsCluster cluster = new TweetsCluster();
    JobTimer.print(() -> {
      cluster.run(options);
      return null;
    });
    System.exit(0);
  }

  private SparkConf sparkConf;

  private TweetsCluster() {
    this(new SparkConf());
  }

  private TweetsCluster(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
//    sparkConf.set("spark.files.maxPartitionBytes", "33554432"); // 32MB
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    // ** Important: GeoMesaSparkKryoRegistrator implementing serialize and de-serialize of SimpleFeature Object
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
//    Class[] classes = new Class[]{NPoint.class, NPoint$.class, NRectRange$.class, NRectRange.class};
//    sparkConf.registerKryoClasses(classes);
    sparkConf.set("spark.kryoserializer.buffer.max", "128m");
    sparkConf.set("spark.network.timeout", "600s");
    sparkConf.set("spark.driver.maxResultSize", "2g");
//    sparkConf.set("spark.sql.crossJoin.enabled","true");
  }

  private void run(TweetsClusterOptions options) {
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Query query = new Query(TweetsFeatureFactory.FT_NAME);
      JavaRDD<SimpleFeature> tweetsRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query);
      int numOfExecutors = sc.sc().getExecutorIds().size() - 1;
      System.out.println("Num of Partition: " + tweetsRDD.getNumPartitions());
      System.out.println("Number of executors: " + numOfExecutors);
      tweetsRDD = tweetsRDD.repartition(numOfExecutors * 2);
      System.out.println("Num of Partition (After Repartition): " + tweetsRDD.getNumPartitions());

      JavaPairRDD<STObject, SimpleFeature> pairRDD = tweetsRDD.mapToPair(sf -> {
        Geometry geom = (Geometry) sf.getDefaultGeometry();
        Date date = (Date) sf.getAttribute(TweetsFeatureFactory.CREATED_AT);
        STObject stObject = STObject.apply(geom, date.getTime());
        return new Tuple2<>(stObject, sf);
      });
      ClassTag<STObject> SOTag = scala.reflect.ClassTag$.MODULE$.apply(STObject.class);
      ClassTag<SimpleFeature> SFTag = scala.reflect.ClassTag$.MODULE$.apply(SimpleFeature.class);
      SpatialRDDFunctions<STObject, SimpleFeature> spatialRDD = SpatialRDD.convertSpatialPlain(pairRDD.rdd(), SOTag, SFTag);
      // TODO auto calculate these parameters
      double kms_per_radian_mel = 87.944d; //6371d;
      double range = 0.1;  // km
      long duration = 10 * 60 * 1000; // milli sec
      double epsilon = calculate_epsilon(range, duration, kms_per_radian_mel);
      double speed = calculate_speed(range, duration, kms_per_radian_mel);
      double qGridCellSize = calculate_epsilon(0.5, 0, kms_per_radian_mel) * 2;   //0.5
      int maxPointsPerCell = 500;
      System.out.println("Range = " + range + " km");
      System.out.println("Duration = " + duration + " milliseconds sec");
      System.out.println("Epsilon = " + epsilon + " degree");
      System.out.println("QGridCellSize = " + qGridCellSize + " degree");
      System.out.println("Max num of Points per cell = " + maxPointsPerCell);
      JavaRDD<Tuple2<STObject, Tuple2<Object, SimpleFeature>>> results =
        spatialRDD
          .cluster(3, epsilon, speed, qGridCellSize, new SerializableFunction1<Tuple2<STObject, SimpleFeature>, String>() {
            @Override
            public String apply(Tuple2<STObject, SimpleFeature> v1) {
              return v1._2().getID();
            }
          }, true, maxPointsPerCell, scala.Option.apply(null))
          .toJavaRDD();

      results.mapToPair(t -> {
        Integer clusterId = (Integer) t._2()._1();
        SimpleFeature sf = t._2()._2();
        return new Tuple2<>(clusterId, sf.getID());
      }).countByKey().forEach((id, num) -> {
        System.out.println("ClusterID: " + id + " has " + num + " points.");
      });


//      System.out.println(tweetsRDD.count());

//      Dataset<Row> tweets = ss.read()
//        .format("geomesa")
//        .options(options.getAccumuloOptions())
//        .option("geomesa.feature", TweetsFeatureFactory.FT_NAME)
//        .load();
//      tweets.show();

    }
  }

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
