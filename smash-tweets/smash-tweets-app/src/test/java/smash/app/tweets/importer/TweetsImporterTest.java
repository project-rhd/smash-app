package smash.app.tweets.importer;

import junit.framework.TestCase;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import smash.data.tweets.gt.TweetsFeatureFactory;
import smash.data.tweets.pojo.Tweet;
import smash.utils.geomesa.FeatureWriterOnSpark;
import smash.utils.geomesa.GeoMesaDataUtils;
import smash.utils.geomesa.GeoMesaOptions;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Yikai Gong on 10/04/17.
 */
public class TweetsImporterTest extends TestCase {
  public static final String dummyTweetStr = "{" +
    "\"id_str\":\"508093847138619392\"," +
    "\"created_at\":\"Sat Sep 06 03:26:27 +0000 2014\"," +
    "\"coordinates\":{" +
    "\"type\":\"Point\"," +
    "\"coordinates\":[144.97145176999998739,-37.807908040000000938]}," +
    "\"text\":\"Beautiful day :)\"," +
    "\"user\":{" +
    "\"screen_name\":\"khangiie\"}," +
    "\"lang\":\"en\"}";

  public static final String dummyTweetStr2 = "{\"id_str\":\"" +
    "819879957954445312\",\"text\":\"▷ take my gear to discover " +
    "something new\uD83C\uDF0C\uD83C\uDF0F\\n\\n#Melbourne \\n#gear" +
    " \\n_\\n#Australia #japanese… https://t.co/aMLIYx2NZd\",\"coordinates\"" +
    ":{\"type\":\"Point\",\"coordinates\":[144.96138889,-37.82055556]}," +
    "\"created_at\":\"Fri Jan 13 12:13:09 +0000 2017\",\"user\":{" +
    "\"screen_name\":\"MihoHariu\"},\"lang\":\"en\"}";

  public static final String expectedJson = "{\"id_str\":\"508093847138619392" +
    "\",\"text\":\"Beautiful day :)\",\"coordinates\":{\"type\":\"Point\"" +
    ",\"coordinates\":[144.97145176999998739,-37.807908040000000938]}," +
    "\"created_at\":\"Sat Sep 06 03:26:27 +0000 2014\",\"user\":{\"" +
    "screen_name\":\"khangiie\"},\"lang\":\"en\",\"tokens\":[\"beautiful\"," +
    "\"day\"],\"sentiment\":1}";

  public void testProcessTweet() throws Exception {
    Tweet dummy = Tweet.fromJSON(dummyTweetStr);
    dummy = TweetsImporter.processTweet(dummy);
    System.out.println(dummy.toJSON());
    assertEquals(dummy.toJSON(), expectedJson);

    Tweet dummy2 = Tweet.fromJSON(dummyTweetStr2);
    dummy2 = TweetsImporter.processTweet(dummy2);
    System.out.println(dummy2.toJSON());
  }
}