package smash.data.tweets.gt;

import com.google.common.base.Joiner;
import com.google.gson.reflect.TypeToken;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import smash.data.tweets.pojo.Tweet;
import smash.data.tweets.pojo.TweetCoordinates;
import smash.data.tweets.pojo.TweetUser;

import java.lang.reflect.Type;
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
  public static final String CLUSTER_ID = "cluster_id";
  public static final String CLUSTER_LABEL = "cluster_label";
  public static final String DAY_OF_WEEK = "day_of_week";
  public static final String SEC_OF_DAY = "sec_of_day";

  public static final String timeFormat = "EEE MMM dd HH:mm:ss Z yyyy";

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
    attributes.add(DAY_OF_WEEK + ":String:index=join");
    attributes.add(SEC_OF_DAY + ":Long:index=join");
    attributes.add(CLUSTER_ID + ":String:index=full"); //full;join
    attributes.add(CLUSTER_LABEL + ":String");

    String simpleFeatureTypeSchema = Joiner.on(",").join(attributes);
    SimpleFeatureType simpleFeatureType =
      SimpleFeatureTypes.createType(FT_NAME, simpleFeatureTypeSchema);
    simpleFeatureType.getUserData()
      .put(SimpleFeatureTypes.DEFAULT_DATE_KEY, CREATED_AT);
    return simpleFeatureType;
  }

  public static SimpleFeature createFeature(Tweet tweet) throws ParseException {
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(SFT);
    SimpleFeature feature = builder.buildFeature("tweet-" + tweet.getId_str());
    // Tell GeoMesa to use user provided FID
    feature.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);
    GeometryFactory geometryFactory =
      new GeometryFactory(new PrecisionModel(), 4326);
    // lon-lat order
    Double lon = tweet.getCoordinates().getLon().doubleValue();
    Double lat = tweet.getCoordinates().getLat().doubleValue();
    Coordinate coordinate = new Coordinate(lon, lat);
    Point point = geometryFactory.createPoint(coordinate);
    // Note: SimpleDateFormat is not thread-safe
    DateFormat df = new SimpleDateFormat(timeFormat);
    Date created_at = df.parse(tweet.getCreated_at());
    String tokenStr = Tweet.gson.toJson(tweet.getTokens());

    DateFormat df2 = new SimpleDateFormat("EE");
    df2.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
    String dayOfWeek_Str = df2.format(created_at);

    df2 = new SimpleDateFormat("yyyy-MM-ddZ");
    df2.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"));
    Date start_of_day = df.parse(df2.format(created_at));
    Long sec_of_day = (created_at.getTime()-start_of_day.getTime())/1000;

    feature.setDefaultGeometry(point);
    feature.setAttribute(ID_STR, tweet.getId_str());
    feature.setAttribute(CREATED_AT, created_at);
    feature.setAttribute(TEXT, tweet.getText());
    feature.setAttribute(SCREEN_NAME, tweet.getUser().getScreen_name());
    feature.setAttribute(TOKENS, tokenStr);
    feature.setAttribute(SENTIMENT, tweet.getSentiment());
    feature.setAttribute(DAY_OF_WEEK, dayOfWeek_Str);
    feature.setAttribute(SEC_OF_DAY, sec_of_day);

    return feature;
  }

  public static Tweet fromSFtoPojo(SimpleFeature sf) {
    Tweet t = new Tweet();
    String[] fid = sf.getID().split("-");
    if (!fid[0].equals("tweet")) return null;
    Double lon = ((Point) sf.getDefaultGeometry()).getX();
    Double lat = ((Point) sf.getDefaultGeometry()).getY();
    DateFormat df = new SimpleDateFormat(timeFormat);
    Date date = (Date) sf.getAttribute(CREATED_AT);

    t.setId_str(fid[1]);
    t.setCreated_at(df.format(date));
    t.setText((String) sf.getAttribute(TEXT));
    t.setCoordinates(new TweetCoordinates(lon, lat));
    t.setUser(new TweetUser((String) sf.getAttribute(SCREEN_NAME)));
    Type listType = new TypeToken<ArrayList<String>>() {
    }.getType();
    t.setTokens(Tweet.gson.fromJson((String) sf.getAttribute(TOKENS), listType));
    t.setSentiment((Integer) sf.getAttribute(SENTIMENT));
    return t;
  }

  public static String getObjId(SimpleFeature sf) {
    String[] fid = sf.getID().split("-");
    if (!fid[0].equals("tweet")) return null;
    return fid[1];
  }
}
