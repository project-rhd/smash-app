package smash.app.tweets.classifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
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
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.data.tweets.pojo.Tweet;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;
import smash.utils.geomesa.GeoMesaWriter;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author Yikai Gong
 */

public class OnStreetTweetsDetector implements Serializable{
  private static Logger logger = LoggerFactory.getLogger(OnStreetTweetsDetector.class);
  private SparkConf sparkConf;

  public OnStreetTweetsDetector() {
    this(new SparkConf());
  }

  public OnStreetTweetsDetector(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    sparkConf.setAppName(this.getClass().getSimpleName());
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{};
    sparkConf.registerKryoClasses(classes);
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    GeoMesaOptions options = new GeoMesaOptions();
    options.parse(args);

    OnStreetTweetsDetector scatsAbnDetector = new OnStreetTweetsDetector();
    JobTimer.print(() -> {
      scatsAbnDetector.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(GeoMesaOptions options) throws IOException, CQLException {
    GeoMesaDataUtils.saveFeatureType(options, TweetsFeatureFactory.SFT_OS);
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      SpatialRDDProvider sp = org.locationtech.geomesa.spark.GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
//      Filter filter = CQL.toFilter("created_at DURING 2017-12-01T00:00:00+11:00/2017-12-01T23:59:59+11:00");
      Query query = new Query(TweetsFeatureFactory.FT_NAME);
      JavaRDD<SimpleFeature> tweetsFeatureRDD = jsp
        .rdd(new Configuration(), sc, options.getAccumuloOptions(), query).repartition(50);

      tweetsFeatureRDD = tweetsFeatureRDD.map(sf->{
        String wkt_point_str = sf.getDefaultGeometry().toString();
//        if(wkt_point_str == null || wkt_point_str.isEmpty())
//          return null;
        String queryStr = "DWITHIN(the_geom, " + wkt_point_str + ", " + 10 + ", meters)";
        Query query_osm = new Query("melOsmLine", CQL.toFilter(queryStr));
        GeoMesaOptions options1 = options.copy();
        options1.setTableName("osm");
        int num_of_street = GeoMesaDataUtils.getNumOfFeatures(options1, query_osm);
//        System.out.println(queryStr);
//        System.out.println(num_of_street);
        Tweet t_pojo = TweetsFeatureFactory.fromSFtoPojo(sf);
        SimpleFeature updatedSf;
        if (num_of_street > 0){
          // It is an on-street tweet;
          updatedSf = TweetsFeatureFactory.createOnStreetFeature(t_pojo, true);

        } else{
          // It is not an on-street tweet;
          updatedSf = TweetsFeatureFactory.createOnStreetFeature(t_pojo, false);
        }
        return updatedSf;
      });

      jsp.save(tweetsFeatureRDD, options.getAccumuloOptions(), TweetsFeatureFactory.FT_NAME_OS);
    }
  }
}
