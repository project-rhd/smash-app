package smash.utils.geomesa;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;

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
}
