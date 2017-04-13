package smash.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Yikai Gong
 */

public class JobTimer {
  // Using Java Generic for passing method as arguments
  // java.util.concurrent.Callable can be used for passing no-arg methods.
  // java.util.function.Function can be used for passing one-arg methods.
  // The <T,R,G,...> on method signature can be seen as a place holder
  // for dynamic Types of input-arguments or/and return type.
  // Ref: https://www.tutorialspoint.com/java/java_generics.htm
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

  public static <T, R> T doSth(Function<T, R> method) {
    return null;
  }
}

