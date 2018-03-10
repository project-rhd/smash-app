package smash.data.scats.pojo;

import java.io.Serializable;
import java.util.Date;

/**
 * @author Yikai Gong
 */

public class ScatsVolume implements Serializable{
  private String nb_scats_site;
  private String nb_detector;
  private Date qt_interval_count;
  private String day_of_week;
  private Integer volume;

  private String geoPointString;
  private String geoLineString;

  public ScatsVolume() {
  }

  public String getNb_scats_site() {
    return nb_scats_site;
  }

  public void setNb_scats_site(String nb_scats_site) {
    this.nb_scats_site = nb_scats_site;
  }

  public String getNb_detector() {
    return nb_detector;
  }

  public void setNb_detector(String nb_detector) {
    this.nb_detector = nb_detector;
  }

  public Date getQt_interval_count() {
    return qt_interval_count;
  }

  public void setQt_interval_count(Date qt_interval_count) {
    this.qt_interval_count = qt_interval_count;
  }

  public String getDay_of_week() {
    return day_of_week;
  }

  public void setDay_of_week(String day_of_week) {
    this.day_of_week = day_of_week;
  }

  public Integer getVolume() {
    return volume;
  }

  public void setVolume(Integer volume) {
    this.volume = volume;
  }

  public String getGeoPointString() {
    return geoPointString;
  }

  public void setGeoPointString(String geoPointString) {
    this.geoPointString = geoPointString;
  }

  public String getGeoLineString() {
    return geoLineString;
  }

  public void setGeoLineString(String geoLineString) {
    this.geoLineString = geoLineString;
  }
}
