package smash.utils.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.opengis.feature.simple.SimpleFeature;
import smash.utils.geomesa.FeatureWriterOnSpark;
import smash.utils.geomesa.GeoMesaOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class FeatureRDDToGeoMesa {

  public static long save(GeoMesaOptions options,
                          JavaRDD<SimpleFeature> featureRDD,
                          JavaSparkContext jsc) {
    JavaRDD<Long> countFeatures = featureRDD.mapPartitions(featureIterator -> {
      long i = 0;
      if (featureIterator.hasNext()) {
        SimpleFeature simpleFeature = featureIterator.next();
        String stfName = simpleFeature.getFeatureType().getTypeName();
        FeatureWriterOnSpark.lazyInit(options, stfName);
        boolean saved = FeatureWriterOnSpark.write(simpleFeature);
        if (saved)
          i++;
      }
      while (featureIterator.hasNext()) {
        SimpleFeature simpleFeature = featureIterator.next();
        boolean saved = FeatureWriterOnSpark.write(simpleFeature);
        if (saved)
          i++;
      }
      return Arrays.asList(i).iterator();
    });
    Long numberOfSavedFeatures = countFeatures.reduce((a, b) -> a+b);

    closeFeatureWriterOnSparkExecutors(jsc);
    return numberOfSavedFeatures;
  }

  public static void closeFeatureWriterOnSparkExecutors(JavaSparkContext jsc){
    List data1 = Arrays.asList(new Integer[]{Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(3)});
    JavaRDD distData1 = jsc.parallelize(data1, 100);
    distData1.foreachPartition((integerIterator) -> {
      FeatureWriterOnSpark.close();
    });
  }

}
