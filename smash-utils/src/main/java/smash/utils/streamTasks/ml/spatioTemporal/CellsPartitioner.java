package smash.utils.streamTasks.ml.spatioTemporal;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Envelope;
import org.apache.arrow.flatbuf.Bool;
import org.geotools.geometry.jts.ReferencedEnvelope3D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Yikai Gong
 */

public class CellsPartitioner implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(CellsPartitioner.class);

  private ClusterCell topCells;
  private Map<String, ClusterCell> cellsMap;
  private boolean finished = false;
  private Double minBbxSize = 0D;
  private Long maxPts = 100L;
  private Long minPts = 0L;
  private Double extendDist = 0D;

  public CellsPartitioner(ClusterCell topCell, Double minBbxSize, Long maxPts, Long minPts, Double extendDist) {
    this.minBbxSize = minBbxSize;
    this.maxPts = maxPts;
    this.minPts = minPts;
    this.topCells = topCell;
    this.extendDist = extendDist;
    cellsMap = new HashMap<>();
  }

  public void doPartition() {
    if (finished)
      logger.warn("Partition has already finished");
    divideOrSave(topCells);
    finished = true;
  }

  private void divideOrSave(ClusterCell inCell) {
    // Drop cells with too little points
//    System.out.println("cell bbx size: " + inCell.getBbxSize());
//    System.out.println("min bbx size: " + minBbxSize);
    if (inCell.getPtsSize() < minPts) {
      return;
    }
    if(!inCell.isContainNewPoints())  //TODO filter cells contain no new input
      return;
    // Continue divide into 4 seb-cells
    else if (inCell.getPtsSize() > maxPts && inCell.getBbxSize() > minBbxSize) {
      List<Map.Entry<Vector<Double>, Boolean>> points = inCell.getPoints();
      ReferencedEnvelope3D envelope = inCell.getBbx();

      double min_x = envelope.getMinX();
      double max_x = envelope.getMaxX();
      double d_x = max_x - min_x;
      double mid_x = min_x + (d_x / 2.0);

      double min_y = envelope.getMinY();
      double max_y = envelope.getMaxY();
      double d_y = max_y - min_y;
      double mid_y = min_y + (d_y / 2.0);

      double min_t = envelope.getMinZ();
      double max_t = envelope.getMaxZ();
      double d_t = max_t - min_t;
      double mid_t = min_t + (d_t / 2.0);

      List<Map.Entry<String, ReferencedEnvelope3D>> childBbxs = new ArrayList<>();
      childBbxs.add(Maps.immutableEntry(inCell.getCellId(), envelope));
      childBbxs = childBbxs.stream().flatMap(entry -> {
        String id = entry.getKey();
        ReferencedEnvelope3D bbx = entry.getValue();
        List<Map.Entry<String, ReferencedEnvelope3D>> res = new ArrayList<>();
        if (d_x > minBbxSize) {
          ReferencedEnvelope3D newEnlp1 = new ReferencedEnvelope3D(min_x, mid_x, bbx.getMinY(), bbx.getMaxY(), bbx.getMinZ(), bbx.getMaxZ(), null);
          String id1 = id + "-1";
          ReferencedEnvelope3D newEnlp2 = new ReferencedEnvelope3D(mid_x, max_x, bbx.getMinY(), bbx.getMaxY(), bbx.getMinZ(), bbx.getMaxZ(), null);
          String id2 = id + "-2";
          res.add(Maps.immutableEntry(id1, newEnlp1));
          res.add(Maps.immutableEntry(id2, newEnlp2));
        } else {
          String id0 = id + "-0";
          res.add(Maps.immutableEntry(id0, bbx));
        }
        return res.stream();
      }).flatMap(entry->{
        String id = entry.getKey();
        ReferencedEnvelope3D bbx = entry.getValue();
        List<Map.Entry<String, ReferencedEnvelope3D>> res = new ArrayList<>();
        if (d_y > minBbxSize) {
          ReferencedEnvelope3D newEnlp1 = new ReferencedEnvelope3D(bbx.getMinX(), bbx.getMaxX(), min_y, mid_y, bbx.getMinZ(), bbx.getMaxZ(), null);
          String id1 = id + "1";
          ReferencedEnvelope3D newEnlp2 = new ReferencedEnvelope3D(bbx.getMinX(), bbx.getMaxX(), mid_y, max_y, bbx.getMinZ(), bbx.getMaxZ(), null);
          String id2 = id + "2";
          res.add(Maps.immutableEntry(id1, newEnlp1));
          res.add(Maps.immutableEntry(id2, newEnlp2));
        } else {
          String id0 = id + "0";
          res.add(Maps.immutableEntry(id0, bbx));
        }
        return res.stream();
      }).flatMap(entry->{
        String id = entry.getKey();
        ReferencedEnvelope3D bbx = entry.getValue();
        List<Map.Entry<String, ReferencedEnvelope3D>> res = new ArrayList<>();
        if (d_t > minBbxSize) {
          ReferencedEnvelope3D newEnlp1 = new ReferencedEnvelope3D(bbx.getMinX(), bbx.getMaxX(), bbx.getMinY(), bbx.getMaxY(), min_t, mid_t, null);
          String id1 = id + "1";
          ReferencedEnvelope3D newEnlp2 = new ReferencedEnvelope3D(bbx.getMinX(), bbx.getMaxX(), bbx.getMinY(), bbx.getMaxY(), mid_t, max_t, null);
          String id2 = id + "2";
          res.add(Maps.immutableEntry(id1, newEnlp1));
          res.add(Maps.immutableEntry(id2, newEnlp2));
        } else {
          String id0 = id + "0";
          res.add(Maps.immutableEntry(id0, bbx));
        }
        return res.stream();
      }).collect(Collectors.toList());

      for (Map.Entry<String, ReferencedEnvelope3D> entry : childBbxs){
        String id = entry.getKey();
        ReferencedEnvelope3D newEnlp = entry.getValue();
        List<Map.Entry<Vector<Double>, Boolean>> newPoints = new ArrayList<>();
        List<Map.Entry<Vector<Double>, Boolean>> leftPoints = new ArrayList<>();
        Boolean containNewPts = false;
        for (Map.Entry<Vector<Double>, Boolean> point : points){
          if (newEnlp.contains(point.getKey().get(0), point.getKey().get(1), point.getKey().get(2))){
            newPoints.add(point);
            if (point.getValue() && !containNewPts)
              containNewPts = true;
          }
          else
            leftPoints.add(point);
        }
        points = leftPoints;
        ClusterCell newCell = new ClusterCell(id, newPoints, newEnlp);
        newCell.setContainNewPoints(containNewPts);
        divideOrSave(newCell);
      }
    }
    // save to map
    else {
      ClusterCell cell = inCell.clearPoints();
      cell.getBbx().expandBy(extendDist * 1.01);
      cellsMap.put(cell.getCellId(), cell);
    }
  }

  public boolean isFinished() {
    return finished;
  }

  public Map<String, ClusterCell> getCellsMap() {
    return cellsMap;
  }
}
