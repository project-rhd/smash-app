package smash.data.tweets.pojo;

import com.google.gson.JsonParser;
import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Yikai Gong on 6/04/17.
 */
public class TweetTest extends TestCase {
  public static String dummyTweetStr = "{" +
    "\"id_str\":\"508093847138619392\"," +
    "\"created_at\":\"Sat Sep 06 03:26:27 +0000 2014\"," +
    "\"coordinates\":{" +
    "\"type\":\"Point\"," +
    "\"coordinates\":[144.97145176999998739,-37.807908040000000938]}," +
    "\"text\":\"Beautiful day :)\"," +
    "\"user\":{" +
    "\"screen_name\":\"khangiie\"}," +
    "\"lang\":\"en\"}";

  public static Tweet createDummyTweet() {
    Tweet t = new Tweet();
    t.setId_str("508093847138619392");
    t.setCreated_at("Sat Sep 06 03:26:27 +0000 2014");
    t.setCoordinates(createDummyCoordinates());
    t.setText("Beautiful day :)");
    t.setUser(createDummyUser());
    t.setLang("en");
    return t;
  }

  public static TweetUser createDummyUser() {
    TweetUser user = new TweetUser();
    user.setScreen_name("khangiie");
    return user;
  }

  public static TweetCoordinates createDummyCoordinates() {
    TweetCoordinates coordinates = new TweetCoordinates();
    coordinates.setType("Point");
    List<BigDecimal> lonLat = new ArrayList<>();
    lonLat.add(new BigDecimal("144.97145176999998739"));
    lonLat.add(new BigDecimal("-37.807908040000000938"));
    coordinates.setCoordinates(lonLat);
    return coordinates;
  }

  public void testFromJSON() throws Exception {
    Tweet t = Tweet.fromJSON(dummyTweetStr);
    String t_json = t.toJSON();
    String exp_json = createDummyTweet().toJSON();
    System.out.println(t_json);
    System.out.println(exp_json);
    System.out.println(dummyTweetStr);
    JsonParser parser = new JsonParser();
    // Compare two json strings by parsing them into JsonElement
    assertEquals(parser.parse(t_json), parser.parse(exp_json));
    assertEquals(parser.parse(t_json), parser.parse(dummyTweetStr));
    for (Field f : Tweet.class.getDeclaredFields()) {
      System.out.println(f.getType().getSimpleName() + " + " + f.getName());
    }
  }


}