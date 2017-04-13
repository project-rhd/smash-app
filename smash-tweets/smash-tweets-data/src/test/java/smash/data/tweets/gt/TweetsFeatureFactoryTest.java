package smash.data.tweets.gt;

import junit.framework.TestCase;
import org.opengis.feature.simple.SimpleFeature;
import smash.data.tweets.pojo.Tweet;
import smash.data.tweets.pojo.TweetTest;

/**
 * Created by Yikai Gong on 10/04/17.
 */
public class TweetsFeatureFactoryTest extends TestCase {
  public void testCreateFeature() throws Exception {
    Tweet dummyTweet = Tweet.fromJSON(TweetTest.dummyTweetStr);
    SimpleFeature tweetFeature =
      TweetsFeatureFactory.createFeature(dummyTweet);
    System.out.println(tweetFeature.toString());
  }
}