package smash.data.tweets.pojo;

import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class Tweet implements Serializable {
  private static final Logger logger = LoggerFactory.getLogger(Tweet.class);

  // Raw data field
  private String id_str;
  private String text;
  private TweetCoordinates coordinates;
  private String created_at;
  private TweetUser user;
  private String lang;

  // Additional field
  private List<String> tokens;
  private Integer sentiment;

  public static final Gson gson = new Gson();

  public static Tweet fromJSON(JsonObject j) {
    return gson.fromJson(j, Tweet.class);
  }

  public static Tweet fromJSON(String jsonStr) throws JsonSyntaxException{
      return gson.fromJson(jsonStr, Tweet.class);
  }

  public String toJSON() {
    return gson.toJson(this);
  }

  public Tweet() {
  }

  public String getId_str() {
    return id_str;
  }

  public void setId_str(String id_str) {
    this.id_str = id_str;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public TweetCoordinates getCoordinates() {
    return coordinates;
  }

  public void setCoordinates(TweetCoordinates coordinates) {
    this.coordinates = coordinates;
  }

  public String getCreated_at() {
    return created_at;
  }

  public void setCreated_at(String created_at) {
    this.created_at = created_at;
  }

  public TweetUser getUser() {
    return user;
  }

  public void setUser(TweetUser user) {
    this.user = user;
  }

  public List<String> getTokens() {
    return tokens;
  }

  public void setTokens(List<String> tokens) {
    this.tokens = tokens;
  }

  public Integer getSentiment() {
    return sentiment;
  }

  public void setSentiment(Integer sentiment) {
    this.sentiment = sentiment;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public boolean isGeoTweet() {
    if (isValid() == false || coordinates == null)
      return false;
    else
      return true;
  }

  public boolean isValid() {
    if (id_str == null)
      return false;
    else
      return true;
  }

}
