package smash.utils.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opengis.feature.simple.SimpleFeature;
import smash.utils.geomesa.GeoMesaWriter;
import smash.utils.geomesa.GeoMesaOptions;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Yikai Gong
 */

public class FeatureRDDToGeoMesa {

  public static long save(GeoMesaOptions options,
                          JavaRDD<SimpleFeature> featureRDD,
                          JavaSparkContext jsc) {
    JavaRDD<Long> countFeatures = featureRDD.mapPartitions(featureIterator -> {
      long i = 0;
      GeoMesaWriter writer = null;
      String stfName = null;
      while (featureIterator.hasNext()) {
        SimpleFeature sf = featureIterator.next();
        if (stfName == null)
          stfName = sf.getFeatureType().getTypeName();
        if (writer == null)
          writer = GeoMesaWriter.getThreadSingleton(options, stfName);
        boolean saved = writer.write(sf);
        if (saved)
          i++;
      }
      if (writer != null)
        writer.flush();
      return Arrays.asList(i).iterator();
    });
    Long numberOfSavedFeatures = countFeatures.reduce((a, b) -> a + b);

//    closeFeatureWriterOnSparkExecutors(jsc);
    return numberOfSavedFeatures;
  }

  public static void closeFeatureWriterOnSparkExecutors(JavaSparkContext jsc) {
    jsc.parallelize(new ArrayList<>())
      .foreachPartition(p -> GeoMesaWriter.getThreadSingleton().close());
  }

}
