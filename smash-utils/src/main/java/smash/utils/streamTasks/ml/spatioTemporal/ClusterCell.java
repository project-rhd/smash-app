package smash.utils.streamTasks.ml.spatioTemporal;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * @author Yikai Gong
 */

public class ClusterCell implements Serializable{
  private String cellId;
  private ArrayList<Vector<Double>> points;
  private Envelope bbx;
  private long finalSize = 0;

  public ClusterCell(String cellId, List<Vector<Double>> points, Envelope bbx) {
    this.cellId = cellId;
    this.points = Lists.newArrayList(points.iterator());
    this.bbx = bbx;
  }

  public String getCellId() {
    return cellId;
  }

  public List<Vector<Double>> getPoints() {
    return points;
  }

  public Envelope getBbx() {
    return bbx;
  }

  public Double getBbxSize() {
    assert (bbx != null);
    Double delta_x = Math.abs(points.get(0).get(0) - points.get(1).get(0));
    Double delta_y = Math.abs(points.get(0).get(1) - points.get(1).get(1));
    return Math.min(delta_x, delta_y);
  }

  public int getPtsSize(){
    return getPoints().size();
  }

  public ClusterCell clearPoints(){
    finalSize = points.size();
    points = new ArrayList<>();
    return this;
  }

  public long getFinalSize() {
    return finalSize;
  }
}
