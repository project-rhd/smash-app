package smash.utils.streamTasks.ml.spatioTemporal;

import com.vividsolutions.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Vector;

/**
 * @author Yikai Gong
 */

public class STObj implements Serializable {
  public static final String LABEL_CORE = "CORE";
  public static final String LABEL_BORDER = "BORDER";

  private String objId;
  private Date timestamp;
  private Vector<Double> coordinates;
  private String clusterID;
  private String clusterLabel;
  private Boolean newInput = true;
//  private SimpleFeature feature;
  private String json;

  public STObj(SimpleFeature feature, Date timestamp, String clusterID, String clusterLabel, String json) {
    assert (feature != null && timestamp != null);
//    this.feature = feature;
    this.timestamp = timestamp;
    objId = feature.getID();
    coordinates = new Vector<>();
    Point geo = (Point) feature.getDefaultGeometry();
    coordinates.add(0, geo.getX());
    coordinates.add(1, geo.getY());

    this.json = json;
  }

  public STObj(String objId, Vector<Double> coordinates, Date timestamp, String clusterID, String clusterLabel, String json) {
    this.objId = objId;
    this.coordinates = coordinates;
    this.timestamp = timestamp;
    this.clusterID = clusterID;
    this.clusterLabel = clusterLabel;
    this.json = json;
  }

  public double getDistanceTo(STObj target, Double spatioTemp_ratio) {
    assert (this.getCoordinates() != null && target != null && target.getCoordinates() != null);
    double kms_per_radian_mel = 87.944d;
    spatioTemp_ratio = spatioTemp_ratio == null ? 0D : spatioTemp_ratio;
    double delta_x = target.getCoordinates().get(0) - this.getCoordinates().get(0);
    double delta_y = target.getCoordinates().get(1) - this.getCoordinates().get(1);
    double delta_z = (target.getTimestamp().getTime() - this.getTimestamp().getTime()) * spatioTemp_ratio / kms_per_radian_mel;
    return Math.sqrt(Math.pow(delta_x, 2) + Math.pow(delta_y, 2) + Math.pow(delta_z, 2));
  }

  public ArrayList<STObj> getNeighbours(Iterator<STObj> pointsItr, Double epsilon, Double spatioTemp_ratio) {
    assert (pointsItr != null);
    epsilon = epsilon == null ? 0d : epsilon;
    ArrayList<STObj> nbList = new ArrayList<>();
    while (pointsItr.hasNext()) {
      STObj candidate = pointsItr.next();
      if (!this.getObjId().equals(candidate.getObjId())) {
        if (candidate.getDistanceTo(this, spatioTemp_ratio) < epsilon)
          nbList.add(candidate);
      }
    }
    return nbList;
  }

  // Get
  public String getObjId() {
    return objId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public Vector<Double> getCoordinates() {
    return coordinates;
  }

  public String getClusterID() {
    return clusterID;
  }

  public String getClusterLabel() {
    return clusterLabel;
  }

  public Boolean isNewInput() {
    return newInput;
  }


  //  public SimpleFeature getFeature() {
//    return feature;
//  }

  // Set
  public void setCoordinates(Vector<Double> coordinates) {
    this.coordinates = coordinates;
  }

  public void setClusterID(String clusterID) {
    this.clusterID = clusterID;
  }

  public void setClusterLabel(String clusterLabel) {
    this.clusterLabel = clusterLabel;
  }

  public String getJson() {
    return json;
  }

  public void setNewInput(Boolean newInput) {
    this.newInput = newInput;
  }

  //  public void setJson(String json) {
//    this.json = json;
//  }

  protected STObj clone(){
//    return new STObj(this.feature, this.timestamp, this.clusterID, this.clusterLabel);
    return new STObj(objId, coordinates, timestamp, clusterID, clusterLabel, json);
  }
}
