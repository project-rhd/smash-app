package smash.data.tweets.pojo;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;


import java.io.Serializable;


/**
 * @author Yikai Gong
 */

public class TweetUser implements Serializable {
  private String screen_name;

  public TweetUser() {
  }

  public TweetUser(String screen_name) {
    this.screen_name = screen_name;
  }

  public String getScreen_name() {
    return screen_name;
  }

  public void setScreen_name(String screen_name) {
    this.screen_name = screen_name;
  }

}
