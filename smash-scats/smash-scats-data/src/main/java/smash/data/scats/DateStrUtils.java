package smash.data.scats;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Yikai Gong
 */

public class DateStrUtils {

  public static String getAusTimeOfDay(Date date){
    SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
    String timeOfDay = df.format(date);
    return timeOfDay;
  }
}
