package smash.data.scats.gt;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.sql.Row;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author Yikai Gong
 */

public class ScatsFeaturePointFactory {
  public static final String FT_NAME = "ScatsVolumeData";
  // Attributes fields
  public static final String GEOMETRY = "geometry";
  public static final String NB_SCATS_SITE = "nb_scats_site";
  public static final String QT_INTERVAL_COUNT = "qt_interval_count";
  public static final String NB_DETECTOR = "nb_detector";
  public static final String DAY_OF_WEEK = "day_of_week";
  public static final String DS_LOCATION = "ds_location";
  public static final String NB_LANE = "nb_lane";
  public static final String LANE_MVT = "lane_mvt";
  public static final String LOC_MVT = "loc_mvt";
  public static final String HF = "hf";
  public static final String UNIQUE_ROAD = "unique_road";
  public static final String VOLUME = "volume";

  public static final String dateFormat = "yyyy-MM-dd HH:mm:ss";

  public static SimpleFeatureType createFeatureType() {
    List<String> attributes = new ArrayList<>();
    attributes.add("*" + GEOMETRY + ":Point:srid=4326");
    attributes.add(NB_SCATS_SITE + ":String:index=join");
    attributes.add(QT_INTERVAL_COUNT + ":Date");
    attributes.add(NB_DETECTOR + ":String");
    attributes.add(DAY_OF_WEEK + ":String:index=join");
    attributes.add(VOLUME + ":Integer");
    attributes.add(DS_LOCATION + ":String");
    attributes.add(NB_LANE + ":String");
    attributes.add(LANE_MVT + ":String");
    attributes.add(LOC_MVT + ":String");
    attributes.add(HF + ":String:index=join");
    attributes.add(UNIQUE_ROAD + ":MultiLineString:srid=4326");

    // create the bare simple-feature type
    String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
    SimpleFeatureType simpleFeatureType =
      SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
    // use the user-data (hints) to specify which date-time field is meant to be indexed;
    simpleFeatureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, QT_INTERVAL_COUNT);
    return simpleFeatureType;
  }

  public static SimpleFeatureBuilder getFeatureBuilder() throws SchemaException, FactoryException {
    SimpleFeatureType simpleFeatureType = createFeatureType();
    return new SimpleFeatureBuilder(simpleFeatureType);
  }

  public static ArrayList<SimpleFeature> buildFeatureFromRow(Row row, SimpleFeatureBuilder builder) throws ParseException {
    ArrayList<SimpleFeature> sfs = new ArrayList<>(96);
    String wktPoint = row.getString(0);
    String wktLine = row.getString(109);
    Point point = null;
    MultiLineString line = null;
    if (wktPoint != null && !wktPoint.equals("null")) {
      point = (Point) WKTUtils.read(wktPoint);
      point.setSRID(4326);
    } else {
      // Since GeoMesa do not allow null for default geometry,
      // Skip this row by return null.
      return null;
    }
    if (wktLine != null && !wktLine.equals("null")) {
      Geometry geo = WKTUtils.read(wktLine);
      if (geo instanceof MultiLineString)
        line = (MultiLineString) geo;
      else if (geo instanceof LineString) {
        LineString[] lineStrings = new LineString[]{(LineString) geo};
        line = new GeometryFactory().createMultiLineString(lineStrings);
      }
      if (line != null)
        line.setSRID(4326);
    }

    String scatsSite = row.getString(1);
    String dateStr = row.getString(2);
    String detectorNum = row.getString(3);
    DateFormat df = new SimpleDateFormat(dateFormat);
    df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
    Date date = df.parse(dateStr);
    String dayOfWeek_Str = new SimpleDateFormat("EE").format(date).toString();

    for (int i = 0; i < 96; i++) {
      builder.reset();
      String fid = scatsSite + "-" + detectorNum + "-" + date.getTime();
      SimpleFeature simpleFeature = builder.buildFeature(fid);
      // Tell GeoMesa to use user provided FID
      simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
      simpleFeature.setAttribute(GEOMETRY, point);
      simpleFeature.setAttribute(NB_SCATS_SITE, scatsSite);
      simpleFeature.setAttribute(QT_INTERVAL_COUNT, date);
      simpleFeature.setAttribute(NB_DETECTOR, detectorNum);
      simpleFeature.setAttribute(DAY_OF_WEEK, dayOfWeek_Str);

      String volume = row.getString(i + 4);
      Integer vol = Integer.parseInt(volume);
//      String feature = i < 10 ? "v0" + Integer.toString(i) : "v" + Integer.toString(i);
      simpleFeature.setAttribute(VOLUME, vol);

      simpleFeature.setAttribute(DS_LOCATION, row.getString(104));  //100
      simpleFeature.setAttribute(NB_LANE, row.getString(105));
      simpleFeature.setAttribute(LANE_MVT, row.getString(106));
      simpleFeature.setAttribute(LOC_MVT, row.getString(107));
      simpleFeature.setAttribute(HF, row.getString(108));
      simpleFeature.setAttribute(UNIQUE_ROAD, line);

      sfs.add(simpleFeature);
      date = DateUtils.addMinutes(date, 15);
    }

    return sfs;
  }

  public static ArrayList<SimpleFeature> buildFeatureFromRow(Row row) throws ParseException, SchemaException, FactoryException {
    SimpleFeatureBuilder builder = getFeatureBuilder();
    return buildFeatureFromRow(row, builder);
  }
}
