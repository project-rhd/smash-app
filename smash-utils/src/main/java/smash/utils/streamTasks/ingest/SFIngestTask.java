package smash.utils.streamTasks.ingest;

import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import smash.utils.streamTasks.AbstractTask;
import smash.utils.streamTasks.StreamTaskWriter;

import java.util.Map;
import java.util.Properties;

/**
 * @author Yikai Gong
 */

public class SFIngestTask<T, U> extends AbstractTask<T, U> {
  public static final String PROP_WRITER = "writer";

  private final Object locker = new Object();
  private boolean doneSetup = false;
  private StreamTaskWriter<T> writer;

  private static final ThreadLocal<SFIngestTask> t =
    ThreadLocal.withInitial(SFIngestTask::new);

  @SuppressWarnings("unchecked")
  public static <T, U> SFIngestTask<T, U> getThreadSingleton(Logger l_, Properties p_) {
    SFIngestTask<T, U> singleton = (SFIngestTask<T, U>) t.get();
    singleton.setup(l_, p_);
    return singleton;
  }

  public SFIngestTask() {
    super();
    System.out.println("new " + this.getClass().getSimpleName());
  }

  public SFIngestTask(Logger l_, Properties p_) {
    super();
    this.setup(l_, p_);
    System.out.println("new " + this.getClass().getSimpleName());
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
  protected Float doTaskLogic(Map<String, T> map) {
    if (!doneSetup || writer == null) {
      l.warn("Task:  " + this.getClass().getSimpleName() + " has not been fully setup");
      return 0F;
    }
    Float result = 1F;
    for (T v : map.values()) {
      try {
        writer.write(v);
      } catch (Exception e) {
        l.error(e.getMessage());
        result = 0F;
      }
    }
    return result;
  }
}
