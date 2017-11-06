package smash.utils.geomesa;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore;
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureStore;
import org.locationtech.geomesa.utils.geotools.FeatureUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smash.utils.streamTasks.StreamTaskWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Yikai Gong
 */

public class GeoMesaWriter implements StreamTaskWriter<SimpleFeature> {
  private static final Logger logger = LoggerFactory.getLogger(GeoMesaWriter.class);
  private static final ThreadLocal<GeoMesaWriter> t =
    ThreadLocal.withInitial(() -> new GeoMesaWriter());

  private final Object locker = new Object();
  private GeoMesaFeatureStore geoMesaFeatureStore = null;
  private org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter geoMesaFeatureWriter = null;

  public static GeoMesaWriter getThreadSingleton(GeoMesaOptions op, String tn)
    throws IOException {
    GeoMesaWriter singleton = t.get();
    singleton.lazyInit(op, tn);
    return singleton;
  }

  public static GeoMesaWriter getThreadSingleton() {
    return t.get();
  }

  public GeoMesaWriter() {
  }

  public GeoMesaWriter(GeoMesaOptions options, String typeName) throws IOException {
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
      if(logger.isDebugEnabled()) logger.debug("Init geoMesaFeatureWriter");
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
  public boolean write(SimpleFeature simpleFeature) {
    synchronized (this.locker) {
      if (geoMesaFeatureWriter != null) {
        try {
          FeatureUtils.copyToWriter(geoMesaFeatureWriter, simpleFeature, false);
          geoMesaFeatureWriter.write();
          return true;
        } catch (IOException e) {
          logger.error(e.getMessage());
          return false;
        }
      } else {
        logger.error("geoMesaFeatureWriter has not been initiated");
        return false;
      }
    }
  }

  public Iterator<SimpleFeature> read(Filter q) {
    if (geoMesaFeatureStore == null) {
      logger.error("geoMesaFeatureStore has not been initiated");
      return new ArrayList<SimpleFeature>().iterator();
    }
    SimpleFeatureCollection collection = null;
    if (q != null)
      collection = geoMesaFeatureStore.getFeatures(q);
    else
      collection = geoMesaFeatureStore.getFeatures();
    return DataUtilities.iterator(collection.features());
  }

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
        if(logger.isDebugEnabled())
          logger.debug("geoMesaFeatureWriter closed");
      }
    }
  }

  public boolean isClose() {
    return geoMesaFeatureWriter == null;
  }
}


