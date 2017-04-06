package smash.data.tweets;

import com.google.common.base.Joiner;
import com.vividsolutions.jts.geom.*;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.json.JSONArray;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class TweetsFeatureFactory {
  public static final String FT_NAME = "Tweet";

  public static final String GEOMETRY = "geometry";
  public static final String ID_STR = "id_str";
  public static final String CREATED_AT = "created_at";
  public static final String TEXT = "text";
  public static final String SCREEN_NAME = "screen_name";
  public static final String TOKENS = "tokens";
  public static final String SENTIMENT = "sentiment";

  public static SimpleFeatureType SFT;

  static{
    TweetsFeatureFactory.SFT = createFeatureType();
  }

  public static SimpleFeatureType createFeatureType() {
    List<String> attributes = new ArrayList<>();
    attributes.add("*" + GEOMETRY + ":Point:srid=4326");
    attributes.add(ID_STR + ":String:index=join");
    attributes.add(CREATED_AT + ":Date");
    attributes.add(TEXT + ":String");
    attributes.add(SCREEN_NAME + ":String");
    attributes.add(TOKENS + ":String");
    attributes.add(SENTIMENT + ":Integer");

    String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
    SimpleFeatureType simpleFeatureType =
      SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
    simpleFeatureType.getUserData()
      .put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "created_at");
    return simpleFeatureType;
  }

  public static SimpleFeature createFeature(Tweet tweet) {
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(SFT);
    SimpleFeature feature = builder.buildFeature("tweet-" + tweet.getId_str());

    GeometryFactory geometryFactory =
      new GeometryFactory(new PrecisionModel(), 4326);
    // lon-lat order
    Coordinate coordinate =
      new Coordinate(tweet.getLongitude(), tweet.getLatitude());
    Point point = geometryFactory.createPoint(coordinate);
    feature.setDefaultGeometry(point);
    feature.setAttribute(ID_STR, tweet.getId_str());
    feature.setAttribute(CREATED_AT, tweet.getCreated_at());
    feature.setAttribute(TEXT, tweet.getText());
    feature.setAttribute(SCREEN_NAME, tweet.getUser().getScreen_name());
    String tokenStr = new JSONArray(tweet.getTokens().toArray()).toString();
    feature.setAttribute(TOKENS, tokenStr);
    feature.setAttribute(SENTIMENT, tweet.getSentiment());

    return feature;
  }

//  public static void main(String[] args) {
//    double lon = 147.832031;
//    double lat = -38.899583;
//    GeometryFactory geometryFactory =
//      new GeometryFactory(new PrecisionModel(), 4326);
//    Coordinate coordinate = new Coordinate(lon, lat);
//    Point point = geometryFactory.createPoint(coordinate);
//    System.out.println(point.toText());
//    System.out.println(point.getSRID());
//  }
}
