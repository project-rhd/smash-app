package smash.data.tweets;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;


/**
 * @author Yikai Gong
 */

public class TwitterUser implements Serializable {
  private String screen_name;

  public static TwitterUser fromJSON (JSONObject j) throws JSONException {
    String name = j.has("screen_name") ? j.getString("screen_name") : null;
    return new TwitterUser(name);
  }

  public JSONObject toJSON(){
    JSONObject json = new JSONObject();
    json.put("screen_name", screen_name);
    return json;
  }

  public TwitterUser() {
  }

  public TwitterUser(String screen_name) {
    this.screen_name = screen_name;
  }

  public String getScreen_name() {
    return screen_name;
  }

  public void setScreen_name(String screen_name) {
    this.screen_name = screen_name;
  }

  @Override
  public String toString() {
    return "smash.tweets.TwitterUser{" +
      "screen_name='" + screen_name + '\'' +
      '}';
  }
}
