package smash.data.scats.gt;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
import org.apache.commons.lang3.time.DateUtils;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.interop.WKTUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author Yikai Gong
 */

public class ScatsNearByFeatureFactory {
  public static final String FT_NAME = "ScatsNearByFeatureFactory";

  public static final String NB_SCATS_SITE = "nb_scats_site";
  public static final String NY_FT_NAME = "feature_name";
  public static final String DAY_OF_WEEK = "day_of_week";
  public static final String TIME_OF_DAY = "time_of_day";
  public static final String AVERAGE = "average";
  public static final String NUM_OF_FEATURES = "num_of_features";
  public static final String STANDARD_DEVIATION = "st_div";
  public static final String MINIMUM_VOLUME = "min_vol";
  public static final String MAXIMUM_VOLUME = "max_vol";

  public static SimpleFeatureType createFeatureType() {
    List<String> attributes = new ArrayList<>();
    attributes.add("*geometry:Point:srid=4326");            //indexed
    attributes.add(NB_SCATS_SITE + ":String:index=full");   //indexed
    attributes.add(DAY_OF_WEEK + ":String:index=full");     //indexed
    attributes.add(TIME_OF_DAY + ":Integer:index=full");     //indexed
    attributes.add(AVERAGE + ":Double");
    attributes.add(STANDARD_DEVIATION + ":Double");
    attributes.add(MINIMUM_VOLUME + ":Double");
    attributes.add(MAXIMUM_VOLUME + ":Double");
    attributes.add(NUM_OF_FEATURES + ":Integer");

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

  public static SimpleFeature buildFeatureFromTuple(Tuple2<String, double[]> tuple) throws ParseException, SchemaException, FactoryException {
    SimpleFeatureBuilder builder = getFeatureBuilder();
    return buildFeatureFromTuple(tuple, builder);
  }

  public static SimpleFeature buildFeatureFromTuple(Tuple2<String, double[]> tuple, SimpleFeatureBuilder builder) throws ParseException {
//    List<SimpleFeature> result = new ArrayList<>();

    String[] keys = tuple._1.split("#");
    String scatsSite = keys[0];
    String geo_wkt = keys[1];
    String dayOfWeek = keys[2];
    String timeOfDay_str = keys[3];

    Point point = (Point) WKTUtils.read(geo_wkt);
    point.setSRID(4326);

    String fid = scatsSite + "-" + dayOfWeek + "-" + timeOfDay_str;
    builder.reset();
    SimpleFeature simpleFeature = builder.buildFeature(fid);
    Integer timeOfDay_int = new Integer(timeOfDay_str.split(":")[0]);
    // Tell GeoMesa to use user provided FID
    simpleFeature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
    simpleFeature.setAttribute("geometry", point);
    simpleFeature.setAttribute(NB_SCATS_SITE, scatsSite);
    simpleFeature.setAttribute(DAY_OF_WEEK, dayOfWeek);
    simpleFeature.setAttribute(TIME_OF_DAY, timeOfDay_int);
    simpleFeature.setAttribute(AVERAGE, tuple._2[0]);
    simpleFeature.setAttribute(NUM_OF_FEATURES, (int)tuple._2[1]);
    if (tuple._2.length > 2) {
      simpleFeature.setAttribute(STANDARD_DEVIATION, tuple._2[2]);
      simpleFeature.setAttribute(MINIMUM_VOLUME, tuple._2[3]);
      simpleFeature.setAttribute(MAXIMUM_VOLUME, tuple._2[4]);
    }
    return simpleFeature;
  }
}
