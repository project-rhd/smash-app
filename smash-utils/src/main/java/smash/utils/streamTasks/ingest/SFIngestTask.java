package smash.utils.streamTasks.ingest;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaFeatureWriter;
import smash.utils.geomesa.GeoMesaOptions;
import smash.utils.streamTasks.AbstractTask;
import smash.utils.streamTasks.StreamTaskWriter;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * @author Yikai Gong
 */

public class SFIngestTask<T> extends AbstractTask {
  public static final String PROP_WRITER = "writer";
  private static SFIngestTask instance;

  private final Object locker = new Object();
  private boolean doneSetup = false;
  private StreamTaskWriter<T> writer;

  public static <U> SFIngestTask<U> createOrGetSingleton(Logger l_, Properties p_) {
    synchronized (PROP_WRITER) {
      if (instance == null) {
        System.out.println("new");
        instance = new SFIngestTask<U>(l_, p_);
      }
    }
    return instance;
  }

  private static final ThreadLocal<SFIngestTask> t = new ThreadLocal<SFIngestTask>(){
    @Override
    protected SFIngestTask initialValue() {
      return new SFIngestTask();
    }
  };

  public static SFIngestTask getThreadSingleton (Logger l_, Properties p_){
    SFIngestTask singleton = t.get();
    singleton.setup(l_, p_);
    return singleton;
  }

  public SFIngestTask() {
    super();
    System.out.println("new "+ this.getClass().getSimpleName());
  }

  public SFIngestTask(Logger l_, Properties p_) {
    super();
    this.setup(l_, p_);
    System.out.println("new "+ this.getClass().getSimpleName());
  }

  public void setup(Logger l_, Properties p_) {
    super.setup(l_, p_);
    synchronized (locker) {
      if (!doneSetup) {
        writer = (StreamTaskWriter) p_.get(PROP_WRITER);
        assert (writer != null);
        doneSetup = true;
      }
    }
  }

  @Override
  protected Float doTaskLogic(Map map) {
    if (!doneSetup || writer == null) {
      l.warn("Task:  " + this.getClass().getSimpleName() + " has not been fully setup");
      return 0F;
    }
    Float result = 1F;
    for (Object v : map.values()) {
      try {
        writer.write((T) v);
      } catch (Exception e) {
        l.error(e.getMessage());
        result = 0F;
      }
    }
    return result;
  }
}
