package smash.data.tweets.gt;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.vividsolutions.jts.geom.*;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import smash.data.tweets.pojo.Tweet;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class TweetsFeatureFactory {
  public static final String FT_NAME = "Tweet";
  // Attributes fields
  public static final String GEOMETRY = "geometry";
  public static final String ID_STR = "id_str";
  public static final String CREATED_AT = "created_at";
  public static final String TEXT = "text";
  public static final String SCREEN_NAME = "screen_name";
  public static final String TOKENS = "tokens";
  public static final String SENTIMENT = "sentiment";

  public static final String timeFormat = "EEE MMM dd HH:mm:ss Z yyyy";
  public static final DateTimeFormatter DTformatter =
    DateTimeFormat.forPattern(timeFormat);
  public static SimpleFeatureType SFT = createFeatureType();

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
    Double lon = tweet.getCoordinates().getLon().doubleValue();
    Double lat = tweet.getCoordinates().getLat().doubleValue();
    Coordinate coordinate = new Coordinate(lon, lat);
    Point point = geometryFactory.createPoint(coordinate);
    DateTime createdAt = DateTime.parse(tweet.getCreated_at(), DTformatter);
    String tokenStr = Tweet.gson.toJson(tweet.getTokens());

    feature.setDefaultGeometry(point);
    feature.setAttribute(ID_STR, tweet.getId_str());
    feature.setAttribute(CREATED_AT, createdAt);
    feature.setAttribute(TEXT, tweet.getText());
    feature.setAttribute(SCREEN_NAME, tweet.getUser().getScreen_name());
    feature.setAttribute(TOKENS, tokenStr);
    feature.setAttribute(SENTIMENT, tweet.getSentiment());

    return feature;
  }
}
