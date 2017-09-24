package smash.utils.streamTasks;

import java.io.IOException;

/**
 * @author Yikai Gong
 */

public interface StreamTaskWriter<T> {

  public boolean write(T data) throws IOException;

  public void flush();

  public void close();

}
