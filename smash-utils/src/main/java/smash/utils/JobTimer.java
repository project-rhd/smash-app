package smash.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Yikai Gong
 */

public class JobTimer {
  public static <T> T print(Callable<T> task) {
    T call = null;
    try {
      long startTime = System.currentTimeMillis();
      call = task.call();
      TimeUnit time = TimeUnit.MILLISECONDS;
      long duration = System.currentTimeMillis() - startTime;

      System.out.println("Finished in " + time.toSeconds(duration) + "S");

      long days = time.toDays(duration);
      duration = duration - TimeUnit.DAYS.toMillis(days);
      long hours = time.toHours(duration);
      duration = duration - TimeUnit.HOURS.toMillis(hours);
      long minutes = time.toMinutes(duration);
      duration = duration - TimeUnit.MINUTES.toMillis(minutes);
      long seconds = time.toSeconds(duration);

      System.out.println("Task finished in " +
        days + "D " +
        hours + "H " +
        minutes + "M " +
        seconds + "S.");
    } catch (Exception e) {
      e.printStackTrace();
    }
    return call;
  }
}

