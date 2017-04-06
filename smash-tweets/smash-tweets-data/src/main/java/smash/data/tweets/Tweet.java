package smash.data.tweets;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Yikai Gong
 */

public class Tweet implements Serializable{
  // Raw data field
  private String id_str;
  private String text;
  private Double latitude;
  private Double longitude;
  private DateTime created_at;
  private TwitterUser user;
  // Additional field
  private List<String> tokens;
  private Integer sentiment;

  public static Tweet fromJSON(JsonObject j) throws ParseException {
    String id, text;
    Double lat, lon;
    DateTime time;
    TwitterUser user;
    JsonObject geo = j.has("geo") ? j.getAsJsonObject("geo") : null;
    JsonArray coordinates = geo != null && geo.has("coordinates") ?
      geo.getAsJsonArray("coordinates") : null;
    String timeStr = j.has("created_at") ?
      j.get("created_at").getAsString() : null;
    final String timeFormat = "EEE MMM dd HH:mm:ss Z yyyy";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat);

    id = j.has("id_str") ?
      j.get("id_str").getAsString() : j.get("id").getAsString();
    text = j.has("text") ? j.get("text").getAsString() : null;
    user = j.has("user") ? TwitterUser.fromJSON(j.getAsJsonObject("user")) : null;
    lat = coordinates != null ? coordinates.get(0).getAsDouble() : null;
    lon = coordinates != null ? coordinates.get(1).getAsDouble() : null;
    time = timeStr != null ? DateTime.parse(timeStr, formatter) : null;

    Tweet t = new Tweet(id, text, lat, lon, time, user);

    if (j.has("tokens")){
      JsonArray tweetTokens = j.getAsJsonArray("tokens");
      List<String> tokens = new ArrayList<String>(tweetTokens.size());
      for (int i = 0; i < tweetTokens.size(); i++)
        tokens.add(tweetTokens.get(i).getAsString());
      t.setTokens(tokens);
    }
    if (j.has("sentiment")){
      t.setSentiment(j.get("sentiment").getAsInt());
    }
    return t;
  }

  public JsonObject toJSON() {
    JsonObject json = new JsonObject();
    JsonArray coordinates = new JsonArray();
    JsonArray tokens = new JsonArray();
    coordinates.add(new JsonPrimitive(this.latitude));
    coordinates.add(new JsonPrimitive(this.longitude));
    this.tokens.forEach(token-> tokens.add(new JsonPrimitive(token)));
    json.add("id", new JsonPrimitive(this.id_str));
    json.add("id_str", new JsonPrimitive(this.id_str));
    json.add("text", new JsonPrimitive(this.text));
    json.add("created_at",new JsonPrimitive(this.created_at.toString()));
    json.add("geo", coordinates);
    json.add("user", user.toJSON());
    json.add("tokens", tokens);
    json.add("sentiment", new JsonPrimitive(sentiment));
    return json;
  }

  // Constructor and setter/getter
  public Tweet() {
  }

  public Tweet(String id_str, String text, Double latitude, Double longitude,
               DateTime created_at, TwitterUser user) {
    this.id_str = id_str;
    this.text = text;
    this.latitude = latitude;
    this.longitude = longitude;
    this.created_at = created_at;
    this.user = user;
  }

  public String getId_str() {
    return id_str;
  }

  public void setId_str(String id_str) {
    this.id_str = id_str;
  }

  public Double getLatitude() {
    return latitude;
  }

  public void setLatitude(Double latitude) {
    this.latitude = latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public void setLongitude(Double longitude) {
    this.longitude = longitude;
  }

  public DateTime getCreated_at() {
    return created_at;
  }

  public void setCreated_at(DateTime created_at) {
    this.created_at = created_at;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public TwitterUser getUser() {
    return user;
  }

  public void setUser(TwitterUser user) {
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

  @Override
  public String toString() {
    return "smash.tweets.Tweet{" +
      "id_str='" + id_str + '\'' +
      ", latitude=" + latitude +
      ", longitude=" + longitude +
      ", created_at=" + created_at +
      ", text='" + text + '\'' +
      ", user=" + user +
      ", tokens=" + tokens +
      ", sentiment=" + sentiment +
      '}';
  }
}
