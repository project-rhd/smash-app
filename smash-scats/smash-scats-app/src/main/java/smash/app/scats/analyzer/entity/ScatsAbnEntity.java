package smash.app.scats.analyzer.entity;

import java.io.Serializable;

/**
 * @author Yikai Gong
 */

public class ScatsAbnEntity implements Serializable{
  private Boolean isScatsAbn = false;
  private Boolean isTweetsAbn = false;
  private Boolean isTweetEqZero = false;
  private Boolean isTweetCluster = false;

  public ScatsAbnEntity() {
  }

  public ScatsAbnEntity(Boolean isScatsAbn, Boolean isTweetsAbn) {
    this.isScatsAbn = isScatsAbn;
    this.isTweetsAbn = isTweetsAbn;
  }

  public Boolean getScatsAbn() {
    return isScatsAbn;
  }

  public void setScatsAbn(Boolean scatsAbn) {
    isScatsAbn = scatsAbn;
  }

  public Boolean getTweetsAbn() {
    return isTweetsAbn;
  }

  public void setTweetsAbn(Boolean tweetsAbn) {
    isTweetsAbn = tweetsAbn;
  }

  public Boolean getTweetEqZero() {
    return isTweetEqZero;
  }

  public void setTweetEqZero(Boolean tweetEqZero) {
    isTweetEqZero = tweetEqZero;
  }

  public Boolean getTweetCluster() {
    return isTweetCluster;
  }

  public void setTweetCluster(Boolean tweetCluster) {
    isTweetCluster = tweetCluster;
  }
}
