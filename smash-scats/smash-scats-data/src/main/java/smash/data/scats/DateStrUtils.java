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

  public static String getAusHourOfDay(Date date){
    String timeOfDay = getAusTimeOfDay(date);
    String hourStr = timeOfDay.split(":")[0] + ":00:00";
    return hourStr;
  }

  public static String getAusDateStr(Date date){
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
    String dateStr = df.format(date);
    return dateStr;
  }
}
