package smash.utils.streamTasks.ml.spatioTemporal;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import org.geotools.geometry.jts.ReferencedEnvelope3D;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * @author Yikai Gong
 */

public class ClusterCell implements Serializable{
  private String cellId;
  private ArrayList<Map.Entry<Vector<Double>, Boolean>> points;
  private ReferencedEnvelope3D bbx;
  private long finalSize = 0;

  public ClusterCell(String cellId, List<Map.Entry<Vector<Double>, Boolean>> points, ReferencedEnvelope3D bbx) {
    this.cellId = cellId;
    this.points = Lists.newArrayList(points.iterator());
    this.bbx = bbx;
  }

  public String getCellId() {
    return cellId;
  }

  public List<Map.Entry<Vector<Double>, Boolean>> getPoints() {
    return points;
  }

  public ReferencedEnvelope3D getBbx() {
    return bbx;
  }

  public Double getBbxSize() {
    assert (bbx != null);
    Double delta_x = bbx.getMaxX() - bbx.getMinX();
    Double delta_y = bbx.getMaxY() - bbx.getMinY();
    Double delta_z = bbx.getMaxZ() - bbx.getMinZ();
    return Math.max(delta_x, Math.max(delta_y, delta_z));
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


  public Boolean containsNewPoint(){
    Boolean containsNesPoint = false;
    for (Map.Entry<Vector<Double>, Boolean> point: points){
      assert (point.getValue() != null);
      if (point.getValue()){
        containsNesPoint = true;
        break;
      }
    }
    return containsNesPoint;
  }
}
