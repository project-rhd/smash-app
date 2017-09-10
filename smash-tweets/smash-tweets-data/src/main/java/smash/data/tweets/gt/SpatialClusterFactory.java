package smash.data.tweets.gt;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class SpatialClusterFactory {
  public static final String FT_NAME = "SpatialCluster";
  // Attributes fields
  public static final String CLUSTER_ID = "cluster_id";
  public static final String GEOMETRY = "geometry";
  public static final String TIMESTAMP = "timestamp";
  public static final String NUM_OF_ELEMENTS = "num_of_elements";

  public static final String timeFormat = "EEE MMM dd HH:mm:ss Z yyyy";

  public static SimpleFeatureType SFT = createFeatureType();

  public static SimpleFeatureType createFeatureType() {
    List<String> attributes = new ArrayList<>();
    attributes.add("*" + GEOMETRY + ":Point:srid=4326");
    attributes.add(TIMESTAMP + ":Date");
    attributes.add(CLUSTER_ID + ":Integer:index=join");
    attributes.add(NUM_OF_ELEMENTS + ":Integer");

    String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
    SimpleFeatureType simpleFeatureType =
      SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
    simpleFeatureType.getUserData()
      .put(SimpleFeatureTypes.DEFAULT_DATE_KEY, TIMESTAMP);
    return simpleFeatureType;
  }

  public static SimpleFeature createFeature(
    Integer clusterId, Double lon, Double lat, Long time, Integer numOfElements) {
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(SFT);
    SimpleFeature feature = builder.buildFeature("cluster-" + clusterId);
    // Tell GeoMesa to use user provided FID
    feature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
    GeometryFactory geometryFactory =
      new GeometryFactory(new PrecisionModel(), 4326);

    Coordinate coordinate = new Coordinate(lon, lat);
    Point point = geometryFactory.createPoint(coordinate);
    // Note: SimpleDateFormat is not thread-safe
    Date timestamp = new Date(time);

    feature.setDefaultGeometry(point);
    feature.setAttribute(TIMESTAMP, timestamp);
    feature.setAttribute(CLUSTER_ID, clusterId);
    feature.setAttribute(NUM_OF_ELEMENTS, numOfElements);

    return feature;
  }
}
