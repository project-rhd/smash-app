package smash.data.tweets;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;

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

  public static Tweet fromJSON(JSONObject j) throws JSONException, ParseException {
    String id, text;
    Double lat, lon;
    DateTime time;
    TwitterUser user;
    JSONObject geo = j.has("geo") ? j.getJSONObject("geo") : null;
    JSONArray coordinates = geo != null && geo.has("coordinates") ?
      geo.getJSONArray("coordinates") : null;
    String timeStr = j.has("created_at") ? j.getString("created_at") : null;
    final String timeFormat = "EEE MMM dd HH:mm:ss Z yyyy";
    DateTimeFormatter formatter = DateTimeFormat.forPattern(timeFormat);

    id = j.has("id_str") ? j.getString("id_str") : j.getString("id");
    text = j.has("text") ? j.getString("text") : null;
    user = j.has("user") ? TwitterUser.fromJSON(j.getJSONObject("user")) : null;
    lat = coordinates != null ? coordinates.getDouble(0) : null;
    lon = coordinates != null ? coordinates.getDouble(1) : null;
    time = timeStr != null ? DateTime.parse(timeStr, formatter) : null;

    Tweet t = new Tweet(id, text, lat, lon, time, user);

    if (j.has("tokens")){
      JSONArray tweetTokens = j.getJSONArray("tokens");
      List<String> tokens = new ArrayList<String>(tweetTokens.length());
      for (int i = 0; i < tweetTokens.length(); i++)
        tokens.add(tweetTokens.getString(i));
      t.setTokens(tokens);
    }
    if (j.has("sentiment")){
      t.setSentiment(j.getInt("sentiment"));
    }
    return t;
  }

  public JSONObject toJSON() {
    JSONObject json = new JSONObject();
    JSONArray coordinates = new JSONArray();
    JSONArray tokens = new JSONArray();
    coordinates.put(this.latitude);
    coordinates.put(this.longitude);
    this.tokens.forEach(tokens::put);
    json.put("id", this.id_str);
    json.put("id_str", this.id_str);
    json.put("text", this.text);
    json.put("created_at",this.created_at.toString());
    json.put("geo", coordinates);
    json.put("user", user.toJSON());
    json.put("tokens", tokens);
    json.put("sentiment", sentiment);
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
