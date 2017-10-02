package smash.data.tweets.pojo;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Yikai Gong
 */

public class TweetCoordinates implements Serializable {
  private String type;
  // Order in lon-lat. Follow the GeoJSON/WKT standard which JTS is holing on
  // Ref: http://www.macwright.org/lonlat/
  // and http://docs.geotools.org/latest/userguide/library/referencing/epsg.html
  private List<BigDecimal> coordinates;

  public TweetCoordinates() {
  }

  public TweetCoordinates(Double lon, Double lat) {
    this();
    type = "point";
    List<BigDecimal> l = new ArrayList<>();
    l.add(0, BigDecimal.valueOf(lon));
    l.add(1, BigDecimal.valueOf(lat));
    coordinates = l;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public List<BigDecimal> getCoordinates() {
    return coordinates;
  }

  public void setCoordinates(List<BigDecimal> coordinates) {
    this.coordinates = coordinates;
  }

  public BigDecimal getLon() {
    return coordinates.get(0);
  }

  public BigDecimal getLat() {
    return coordinates.get(1);
  }


}
