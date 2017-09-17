package smash.utils.geomesa;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureStore;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.utils.streamTasks.StreamTaskWriter;

import java.io.IOException;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class GeoMesaFeatureWriter implements StreamTaskWriter<SimpleFeature> {
  private static final Logger logger = LoggerFactory.getLogger(GeoMesaFeatureWriter.class);
  private static GeoMesaFeatureWriter instance = null;

  private final Object locker = new Object();
  private GeoMesaFeatureStore geoMesaFeatureStore = null;
  private org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter geoMesaFeatureWriter = null;

  public static GeoMesaFeatureWriter createOrGetSingleton(GeoMesaOptions options, String typeName) throws IOException {
    synchronized (logger) {
      if (instance == null)
        instance = new GeoMesaFeatureWriter(options, typeName);
      else
        instance.lazyInit(options, typeName);
    }
    return instance;
  }

  public static void purgeSingleton() {
    synchronized (logger) {
      if (instance != null) {
        instance.close();
        instance = null;
      }
    }
  }

  public GeoMesaFeatureWriter(GeoMesaOptions options, String typeName) throws IOException {
    synchronized (this.locker) {
      this.lazyInit(options, typeName);
    }
  }

  /**
   * Initiate singleton objects lazily
   *
   * @param options  Parameters for Accumulo Connection
   * @param typeName FeatureType name
   * @throws IOException
   */
  private void lazyInit(GeoMesaOptions options, String typeName)
    throws IOException {
    if (geoMesaFeatureStore == null) {
      DataStore dataStore =
        DataStoreFinder.getDataStore(options.getAccumuloOptions());
      geoMesaFeatureStore =
        (GeoMesaFeatureStore) dataStore.getFeatureSource(typeName);
    }
    if (geoMesaFeatureWriter == null) {
      logger.info("Init geoMesaFeatureWriter");
      geoMesaFeatureWriter = ((GeoMesaDataStore) geoMesaFeatureStore
        .getDataStore()).getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT);
    }
  }

  /**
   * Write single feature into Accumulo/GeoMesa without closing connection
   *
   * @param simpleFeature
   * @throws IOException
   */
  public boolean write(SimpleFeature simpleFeature) throws IOException {
    synchronized (this.locker) {
      if(geoMesaFeatureWriter != null){
        FeatureUtils.copyToWriter(geoMesaFeatureWriter, simpleFeature, false);
        geoMesaFeatureWriter.write();
        return true;
      }
    }
    return false;
  }

//  /**
//   * Write features into Accumulo/GeoMesa without closing connection
//   *
//   * @param simpleFeatures
//   * @throws IOException
//   */
//  public static void writeFeatures(SimpleFeatureCollection simpleFeatures) throws IOException {
////    for (SimpleFeature feature : simpleFeatures) {
////      write(feature);
////    }
//    geoMesaFeatureStore.addFeatures(simpleFeatures);
//  }

  /**
   * Flush data into Accumulo
   */
  public void flush() {
    if (geoMesaFeatureWriter != null) {
      geoMesaFeatureWriter.flush();
    }
  }

  /**
   * Flush Data then Close FeatureWriter and FeatureStore
   */
  public void close() {
    synchronized (this.locker) {
      if (geoMesaFeatureWriter != null) {
        geoMesaFeatureWriter.close();
        geoMesaFeatureWriter = null;
        geoMesaFeatureStore = null;
        logger.info("geoMesaFeatureWriter closed");
      }
    }
  }

  public boolean isClose(){
      return geoMesaFeatureWriter == null;
  }
}


