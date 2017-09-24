package smash.utils.streamTasks.ml.spatioTemporal;

import com.vividsolutions.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

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
    if(finished)
      logger.warn("Partition has already finished");
    divideOrSave(topCells);
    finished = true;

  }

  private void divideOrSave(ClusterCell inCell) {
    // Drop cells with too little points
    if (inCell.getPtsSize() < minPts)
      return;
      // Continue divide into 4 seb-cells
    else if (inCell.getPtsSize() > maxPts && inCell.getBbxSize() > 1 * minBbxSize) {
      String cellId = inCell.getCellId();
      List<Vector<Double>> points = inCell.getPoints();
      Envelope envelope = inCell.getBbx();
      double min_x = envelope.getMinX();
      double max_x = envelope.getMaxX();
      double mid_x = min_x + ((max_x - min_x) / 2.0);
      double min_y = envelope.getMinY();
      double max_y = envelope.getMaxY();
      double mid_y = min_y + ((max_y - min_y) / 2.0);
      for (int i = 0; i < 4; i++) {
        String newCellId = cellId + String.valueOf(i);
        Envelope newEnlp = null;
        if (i == 0) newEnlp = new Envelope(min_x, mid_x, min_y, mid_y);
        else if (i == 1) newEnlp = new Envelope(mid_x, max_x, min_y, mid_y);
        else if (i == 2) newEnlp = new Envelope(mid_x, max_x, mid_y, max_y);
        else newEnlp = new Envelope(min_x, mid_x, mid_y, max_y);
        List<Vector<Double>> newPoints = new ArrayList<>();
        List<Vector<Double>> leftPoints = new ArrayList<>();
        for(int j = 0; j < points.size(); j++) {
          Vector<Double> point = points.remove(j);
          if (newEnlp.contains(point.get(0), point.get(1)))
            newPoints.add(point);
          else
            leftPoints.add(point);
        }
        points = leftPoints;
        ClusterCell newCell = new ClusterCell(newCellId, newPoints, newEnlp);
        // inner self Call
        divideOrSave(newCell);
      }
    }
    // save to map
    else {
      ClusterCell cell = inCell.clearPoints();
      cell.getBbx().expandBy(extendDist);
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
