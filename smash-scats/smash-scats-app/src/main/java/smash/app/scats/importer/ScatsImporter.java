package smash.app.scats.importer;

import com.google.common.base.Joiner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.spark.api.java.JavaSpatialRDDProvider;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.data.scats.gt.ScatsFeaturePointFactory;
import smash.data.scats.pojo.SiteLayouts;
import smash.utils.JobTimer;
import smash.utils.geomesa.GeoMesaDataUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.*;

/**
 * @author Yikai Gong
 * Spark Task for ingesting SCATS volume data into GeoMesa/Accumulo
 */

public class ScatsImporter implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(ScatsImporter.class);
  private SparkConf sparkConf;

  // Constructors
  public ScatsImporter() {
    this(new SparkConf());
  }

  public ScatsImporter(SparkConf sparkConf) {
    this.sparkConf = sparkConf.setAppName(this.getClass().getSimpleName());
    // Use Kryo serialization
    this.sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryo.registrator", GeoMesaSparkKryoRegistrator.class.getName());
    Class[] classes = new Class[]{HashMap.class, String.class, Row.class, SiteLayouts.class};
    this.sparkConf.registerKryoClasses(classes);
  }

  public static void main(String[] args)
    throws IllegalAccessException, CmdLineException, NoSuchFieldException {
    ScatsImporterOptions options = new ScatsImporterOptions();
    options.parse(args);
    ScatsImporter importer = new ScatsImporter();
    JobTimer.print(() -> {
      importer.run(options);
      return null;
    });
    System.exit(0);
  }

  private void run(ScatsImporterOptions options) throws IOException {
    // Ensures Feature Type is saved in GeoMesa
    GeoMesaDataUtils.saveFeatureType(options, ScatsFeaturePointFactory.createFeatureType());
    try (SparkSession ss = SparkSession.builder().config(sparkConf).getOrCreate()) {
      JavaSparkContext sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      // Configures the number of partitions to use when shuffling data for joins or aggregations.
      // Here it is the numOfPartition of the resultsRDD returned by ss.sql(sql)
      // 5000 works fine
      ss.sqlContext().setConf("spark.sql.shuffle.partitions", "5000"); // 5000 200

      // Read shape file from hdfs and parse into a hash-map
      HashMap<String, String> scatsPointShp =
        readWktCsv("hdfs://scats-1-master:9000", options.inputPointShapeFile, "point");
      HashMap<String, String> scatsLineShp =
        readWktCsv("hdfs://scats-1-master:9000", options.inputLineShapeFile, "line");
      Broadcast<HashMap> scatsPointShp_b = sc.broadcast(scatsPointShp);
      Broadcast<HashMap> scatsLineShp_b = sc.broadcast(scatsLineShp);

      // 1. Process volume data
      long size = SizeEstimator.estimate(sc.textFile(options.inputVolumeCSV));
      System.out.println("Estimated size of rawVolumeData is " + size);
      JavaRDD<String> rawVolumeData = sc.textFile(options.inputVolumeCSV, 15000); //15000 100
      // Map volumeData from RDD<String> to RDD<Row> and add geo location to each row
      String volumeData_head = rawVolumeData.first();
      JavaRDD<Row> volumeDataRDD = rawVolumeData.mapPartitionsWithIndex((idx, iter) -> {
        // Skip file header (first line of partition_1)
        if (idx == 0)
          iter.next();
        ArrayList<Row> result = new ArrayList<>();
        while (iter.hasNext()) {
          String line = iter.next();
          String cleanedLine = RawVolumeDataCleaner.cleanScatsTupleStr(line);
          if (cleanedLine != null) {
            String[] fields = cleanedLine.split(",");
            String nb_scats_site = fields[0];
            String wktGeometry = (String) scatsPointShp_b.value().get(nb_scats_site);
//          if (wktGeometry != null) {
            cleanedLine = wktGeometry + "," + Joiner.on(",").join(fields);
            fields = cleanedLine.split(",");
            result.add(RowFactory.create(fields));
//          }
          }
        }
        return result.iterator();
      }, false);
      // Convert volumeData from RDD<Row> to Dataset<Row>
      List<StructField> volume_field_names = new ArrayList<>();
      volume_field_names.add(DataTypes.createStructField("wktGeometry", DataTypes.StringType, true));
      for (String fieldName : volumeData_head.split(",")) {
        fieldName = fieldName.substring(1, fieldName.length() - 1);
        volume_field_names.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
      }
      StructType VolumeDataSchema = DataTypes.createStructType(volume_field_names);
      Dataset<Row> schemaVolumeData = ss.createDataFrame(volumeDataRDD, VolumeDataSchema);
//      schemaVolumeData.persist(StorageLevel.MEMORY_ONLY_SER());
      schemaVolumeData.createOrReplaceTempView("volumeData");

      // 2. Process siteLayOut data
      long size_2 = SizeEstimator.estimate(sc.textFile(options.inputLayoutCSV));
      System.out.println("Estimated size of siteLayoutsStr is " + size_2);
      JavaRDD<String> siteLayoutsStr = sc.textFile(options.inputLayoutCSV, 4000); //4000 200
//      siteLayoutsStr.persist(StorageLevel.MEMORY_ONLY_SER());
      // Map layoutData from RDD<String> to RDD<SiteLayouts>
      JavaRDD<SiteLayouts> siteLayoutsRDD = siteLayoutsStr.mapPartitionsWithIndex((idx, iter) -> {
        if (idx == 0)
          iter.next();
        List<SiteLayouts> result = new ArrayList<>();
        while (iter.hasNext()) {
          String[] fields = RawVolumeDataCleaner.removeQuotation(iter.next().split(","));
          String HF_WKT = (String) scatsLineShp_b.value().get(fields[9]);
          result.add(new SiteLayouts(fields[0], fields[3], fields[4],
            fields[5], fields[6], fields[7], fields[8], fields[9], HF_WKT));
        }
        return result.iterator();
      }, false);
      // Convert layoutData from RDD<SiteLayouts> to Dataset<Row>
      Dataset<Row> schemaSiteLayouts = ss.createDataFrame(siteLayoutsRDD, SiteLayouts.class);
      schemaSiteLayouts.createOrReplaceTempView("siteLayouts");

      // 3. SQL operations on Dataset
      String sql =
        "SELECT b.*, " +
          "c.DS_LOCATION, c.NB_LANE, c.LANE_MVT, c.LOC_MVT, c.ID_HOMOGENEOUS_FLOW, c.HF_WKT " +
          "FROM volumeData b " +
          "LEFT JOIN (" +
          "SELECT NB_SCATS_SITE, NB_DETECTOR, " +
          "first_value(DS_LOCATION) as DS_LOCATION, " +
          "first_value(NB_LANE) as NB_LANE, " +
          "first_value(LANE_MVT) as LANE_MVT, " +
          "first_value(LOC_MVT) as LOC_MVT, " +
          "first_value(ID_HOMOGENEOUS_FLOW) as ID_HOMOGENEOUS_FLOW, " +
          "first_value(HF_WKT) as HF_WKT " +
          "FROM siteLayouts " +
          "GROUP BY NB_SCATS_SITE, NB_DETECTOR) c " +
          "ON b.NB_SCATS_SITE = c.NB_SCATS_SITE " +
          "AND b.NB_DETECTOR = c.NB_DETECTOR";
//                "AND b.QT_INTERVAL_COUNT = c.DT_GENERAL";
      Dataset<Row> scatsData = ss.sql(sql);
      scatsData.createOrReplaceTempView("scats");
//        scatsData.persist(StorageLevel.MEMORY_AND_DISK_SER());

      JavaRDD<SimpleFeature> resultRDD = scatsData.javaRDD().mapPartitionsWithIndex((idx, iter) -> {
        ArrayList<SimpleFeature> result = new ArrayList<>();
        SimpleFeatureBuilder featureBuilder = ScatsFeaturePointFactory.getFeatureBuilder();
        while (iter.hasNext()) {
          SimpleFeature simpleFeature =
            ScatsFeaturePointFactory.buildFeatureFromRow(iter.next(), featureBuilder);
          if (simpleFeature != null)
            result.add(simpleFeature);
        }
        return result.iterator();
      }, false);
      SpatialRDDProvider sp = GeoMesaSpark.apply(options.getAccumuloOptions2());
      JavaSpatialRDDProvider jsp = new JavaSpatialRDDProvider(sp);
      Map<String, String> geoMesaOpts = options.getAccumuloOptions();
//      geoMesaOpts.put("generateStats", Boolean.FALSE.toString());
      jsp.save(resultRDD, geoMesaOpts, ScatsFeaturePointFactory.FT_NAME);
    } // end of try block
  }


  private static HashMap<String, String> readWktCsv(String hdfsSource, String path, String type) {
    HashMap<String, String> scatsShp = new HashMap();
    try {
      Configuration configuration = new Configuration();
      configuration.set("fs.defaultFS", hdfsSource);
      FileSystem fs = FileSystem.get(configuration);
      FSDataInputStream inputStream = fs.open(new Path(path));
      Reader reader = new InputStreamReader(inputStream, "UTF-8");
      CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader());

      for (CSVRecord record : parser) {
        String wkt = record.get("WKT");
        if (type.equals("point")) {
          String site_num = record.get("SITE_NO");
          scatsShp.put(site_num, wkt);
        } else if (type.equals("line")) {
          String hfidl = record.get("hfidl");
          String hfidr = record.get("hfidr");
          scatsShp.put(hfidl, wkt);
          scatsShp.put(hfidr, wkt);
        }
      }

      reader.close();
      inputStream.close();
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert (scatsShp.size() != 0);
    return scatsShp;
  }
}