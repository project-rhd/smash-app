package smash.utils.geomesa;

import com.google.common.collect.Lists;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

/**
 * @author Yikai Gong
 */

public class GeoMesaDataUtils {

  public static void saveFeatureType(GeoMesaOptions options, SimpleFeatureType sft) throws IOException {
    DataStore dataStore = DataStoreFinder.getDataStore(options.getAccumuloOptions());
    // Remove feature type with the same name if allow overwrite
    if (options.overwrite && dataStore.getSchema(sft.getTypeName()) != null) {
      dataStore.removeSchema(sft.getTypeName());
    }
    // Save sft to store/db if stf with the same name does not exit
    if (dataStore.getSchema(sft.getTypeName()) == null) {
      dataStore.createSchema(sft);
    }
  }

  public static ReferencedEnvelope getBoundingBox(GeoMesaOptions options, Query query) throws IOException {
    DataStore dataStore = DataStoreFinder.getDataStore(options.getAccumuloOptions());
    SimpleFeatureSource dataSource = dataStore.getFeatureSource(query.getTypeName());
    ReferencedEnvelope envelope;
    envelope = query == null ? dataSource.getBounds() : dataSource.getBounds(query);
    return envelope;
  }

//  public static int getNumOfFeatures(GeoMesaOptions options, Query query) throws IOException {
//    DataStore dataStore = DataStoreFinder.getDataStore(options.getAccumuloOptions());
//    SimpleFeatureSource dataSource = dataStore.getFeatureSource(query.getTypeName());
//    SimpleFeatureIterator itr = dataSource.getFeatures(query).features();
//    int count = 0;
//    try {
//      while (itr.hasNext()) {
//        SimpleFeature sf = itr.next();
//        System.out.println(sf);
//        count++;
//      }
//    } finally {
//      itr.close();
//    }
//    return count;
//  }

  public static ArrayList<SimpleFeature> getFeatures(GeoMesaOptions options, Query query) throws IOException {
    ArrayList<SimpleFeature> list = new ArrayList<>();
    DataStore dataStore = DataStoreFinder.getDataStore(options.getAccumuloOptions());
    SimpleFeatureSource dataSource = dataStore.getFeatureSource(query.getTypeName());
    SimpleFeatureIterator itr = dataSource.getFeatures(query).features();
    try {
      while (itr.hasNext()) {
        SimpleFeature sf = itr.next();
        list.add(sf);
      }
    } finally {
      itr.close();
    }
    return list;
  }
}
