package smash.data.tweets;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;


import java.io.Serializable;


/**
 * @author Yikai Gong
 */

public class TwitterUser implements Serializable {
  private String screen_name;

  public static TwitterUser fromJSON (JsonObject j)   {
    String name = j.has("screen_name") ?
      j.get("screen_name").getAsString() : null;
    return new TwitterUser(name);
  }

  public JsonObject toJSON(){
    JsonObject json = new JsonObject();
    json.add("screen_name", new JsonPrimitive(screen_name));
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
