package smash.data.scats.gt;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class ScatsDOWFeatureFactory {

  public static final String FT_NAME = "ScatsDayOfWeekBySite";

  public static final String NB_SCATS_SITE = "nb_scats_site";
  public static final String NB_DETECTOR = "nb_detector";
  public static final String DAY_OF_WEEK = "day_of_week";
  public static final String TIME_OF_DAY = "time_of_day";
  public static final String AVERAGE_VEHICLE_COUNT = "average_vehicle_count";
  public static final String NUM_OF_FEATURES = "num_of_features";

  public static SimpleFeatureType createFeatureType() {
    List<String> attributes = new ArrayList<>();
    attributes.add("*geometry:Point:srid=4326");            //indexed
    attributes.add(NB_SCATS_SITE + ":String:index=full");   //indexed
    attributes.add(NB_DETECTOR + ":String");
    attributes.add(DAY_OF_WEEK + ":String:index=full");     //indexed
    attributes.add(TIME_OF_DAY + ":Date");
    attributes.add(AVERAGE_VEHICLE_COUNT + ":Double");
    attributes.add(NUM_OF_FEATURES + ":Integer");
    attributes.add("unique_road:MultiLineString:srid=4326");

    // create the bare simple-feature type
    String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
    SimpleFeatureType simpleFeatureType =
      SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
    // use the user-data (hints) to specify which date-time field is meant to be indexed;
    simpleFeatureType.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, TIME_OF_DAY);
    return simpleFeatureType;
  }

  public static SimpleFeatureBuilder getFeatureBuilder() throws SchemaException, FactoryException {
    SimpleFeatureType simpleFeatureType = createFeatureType();
    return new SimpleFeatureBuilder(simpleFeatureType);
  }

  public static List<SimpleFeature> buildFeaturesFromTuple(Tuple2<String, double[]> tuple, SimpleFeatureBuilder builder) throws ParseException {
    List<SimpleFeature> result = new ArrayList<>();

    String[] keys = tuple._1.split("#");
    String scatsSite = keys[0];
    String detectorNum = keys[1];
    String dayOfWeek = keys[2];
    String geo_wkt = keys[3];
    String geo_wkt_line = keys[4];

    Point point = (Point) WKTUtils.read(geo_wkt);
    MultiLineString line = null;
    if (geo_wkt_line != null && !geo_wkt_line.equals("null")) {
      line = (MultiLineString) WKTUtils.read(geo_wkt_line);
      line.setSRID(4326);
    }
    point.setSRID(4326);

    DateFormat df = new SimpleDateFormat("mm");
    for (int i = 0; i < 96; i++) {
      Date timeOfDay = df.parse(Integer.toString(i * 15));
      String fid = scatsSite + "-" + detectorNum + "-" + dayOfWeek + "-" + timeOfDay;
      builder.reset();
      SimpleFeature simpleFeature = builder.buildFeature(fid);
      // Tell GeoMesa to use user provided FID
      simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
      simpleFeature.setAttribute("geometry", point);
      simpleFeature.setAttribute(NB_SCATS_SITE, scatsSite);
      simpleFeature.setAttribute(NB_DETECTOR, detectorNum);
      simpleFeature.setAttribute(DAY_OF_WEEK, dayOfWeek);
      simpleFeature.setAttribute(TIME_OF_DAY, timeOfDay);
      simpleFeature.setAttribute(AVERAGE_VEHICLE_COUNT, tuple._2[i]);
      simpleFeature.setAttribute(NUM_OF_FEATURES, tuple._2[96]);
      simpleFeature.setAttribute("unique_road", line);
      result.add(simpleFeature);
    }
    return result;
  }

  public static List<SimpleFeature> buildFeaturesFromTuple(Tuple2<String, double[]> tuple) throws ParseException, SchemaException, FactoryException {
    SimpleFeatureBuilder builder = getFeatureBuilder();
    return buildFeaturesFromTuple(tuple, builder);
  }
}
