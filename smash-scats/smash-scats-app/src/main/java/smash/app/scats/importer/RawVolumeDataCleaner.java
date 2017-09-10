package smash.app.scats.importer;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Yikai Gong
 */

public class RawVolumeDataCleaner {

  /**
   * Return a cleaned line of scats data by input a raw data tuple string line.
   * Return null when input line is to be ipgnored.
   *
   * @param rawLine {String}
   * @return cleanedLine {String}
   */
  public static String cleanScatsTupleStr(String rawLine) {
    String[] fields = removeQuotation(rawLine.split(","));
    boolean ignore = true;
    for (int i = 3; i < fields.length; i++) {
      String value = fields[i];
      if (value.equals("") || Integer.parseInt(value) < 0) {
        fields[i] = "0";
      } else {
        fields[i] = value;
        ignore = false;
      }
    }
    String cleanedLine = null;
    if (!ignore) {
      cleanedLine = StringUtils.join(fields, ',');
    }
    return cleanedLine;
  }

  public static String[] removeQuotation(String[] fields) {
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      if (field.charAt(0) == '"' && field.charAt(field.length() - 1) == '"') {
        fields[i] = field.substring(1, field.length() - 1);
      }
    }
    return fields;
  }


}