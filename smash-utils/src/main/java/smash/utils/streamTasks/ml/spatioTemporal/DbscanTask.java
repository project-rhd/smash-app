package smash.utils.streamTasks.ml.spatioTemporal;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import smash.utils.geomesa.GeoMesaWriter;
import smash.utils.streamTasks.AbstractTask;
import smash.utils.streamTasks.StreamTaskWriter;

import java.util.*;

/**
 * @author Yikai Gong
 */

public class DbscanTask<T extends Vector<Double>, U> extends AbstractTask<T, U> {
  // Static fields
  public static final String PROP_NxtCluID = "nxtCluId",
    PROP_MinPTS = "minPts", PROP_DistTime = "dist_time",
    PROP_DistSpatial = "dist_spatial",
    PROP_TimeSpatialRatio = "timeSpatial_ratio", PROP_DBAgent = "dbAgent";

  private static final ThreadLocal<DbscanTask> t =
    ThreadLocal.withInitial(DbscanTask::new);

  @SuppressWarnings("unchecked")
  public static DbscanTask getThreadSingleton(Logger l_, Properties p_) {
    DbscanTask singleton = t.get();
    singleton.setup(l_, p_);
    return singleton;
  }

  // Instance fields
  private boolean doneSetup = false;

  private GeoMesaWriter dbAgent;
  // Next cluster Id
  private Long nxtCluId;
  // Min number of points to form a cluster
  private Long minPts;
  // Time-Spatial epsilon
  private Long dist_time;
  private Double dist_spatial;
  private Double timeSpatial_ratio;
  private Double epsilon;

  private Double eDivR;

  public DbscanTask() {
  }

  public DbscanTask(Logger l_, Properties p_) {
    this();
    this.setup(l_, p_);
  }

  @Override
  //TODO different version of clustering?
  public void setup(Logger l_, Properties p_) {
    if (doneSetup) return;
    super.setup(l_, p_);
    try {
      nxtCluId = Long.valueOf(p_.getProperty(PROP_NxtCluID));
      minPts = Long.valueOf(p_.getProperty(PROP_NxtCluID));
      dist_time = Long.valueOf(p_.getProperty(PROP_DistTime));
      dist_spatial = Double.valueOf(p_.getProperty(PROP_DistSpatial));
      timeSpatial_ratio = Double.valueOf(p_.getProperty(PROP_TimeSpatialRatio));
      epsilon = Math.sqrt(
        Math.pow(dist_spatial, 2) + Math.pow(dist_time * timeSpatial_ratio, 2)
      );
      eDivR = epsilon / timeSpatial_ratio;
      dbAgent = (GeoMesaWriter) p_.get(PROP_DBAgent);
      doneSetup = true;
    } catch (NumberFormatException | ClassCastException e) {
      l.error(e.getMessage());
      doneSetup = false;
    }

  }

  @Override
  protected Float doTaskLogic(Map<String, T> map) {
//    if (!doneSetup || dbAgent == null) {
//      l.warn("Task:  " + this.getClass().getSimpleName()
//        + " has not been fully setup");
//      return 0F;
//    }
//    Map.Entry<String, T> entry = map.entrySet().iterator().next();
//    String ptId = entry.getKey();
//    T coordinate = entry.getValue();
//    Double lon = coordinate.get(0);
//    Double lat = coordinate.get(1);
//    Filter filter = null;
//    try {
//      filter = CQL.toFilter("DWITHIN(geometry, POINT(144.96483935 -37.8177187), 2, kilometers) and created_at DURING 2016-12-28T16:06:06.000Z/2016-12-28T17:06:06.000Z");
//    } catch (CQLException e) {
//      l.warn(e.getMessage());
//      return 0F;
//    }
//    Iterator<SimpleFeature> itr = dbAgent.read(filter);
//    System.out.println(Iterators.size(itr));
//    itr.forEachRemaining(sf->{
//      System.out.println(sf.getID());
//    });


    return null;
  }

  public static Map<String, STObj> localDBSCAN(Iterable<STObj> stObjItrFac, Double epsilon, Double spatioTemp_ratio, Long minPts) {
    Map<String, STObj> toBeUpdated = new HashMap<>();
    Map<String, STObj> Resource = new HashMap<>();
    ArrayList<STObj> localPointList = Lists.newArrayList(stObjItrFac.iterator());
    stObjItrFac.iterator().forEachRemaining(stObj -> {
      // For each un-flagged non-cluster point
      if ((stObj.getClusterID() == null || stObj.getClusterID().equals(""))) {
        extendFromSeed(stObj, localPointList, toBeUpdated, epsilon, spatioTemp_ratio, minPts);
      } else {
        String key = stObj.getClusterID() + "#" + stObj.getObjId();
        // Avoid overwrite updated cluster label
        toBeUpdated.putIfAbsent(key, stObj);
      }
    });
    return toBeUpdated;
  }

  public static void extendFromSeed(STObj seed, List<STObj> staticPointList, Map<String, STObj> toBeUpdated, Double epsilon, Double spatioTemp_ratio, Long minPts) {
    assert (seed.getClusterID() == null && seed.getClusterLabel() == null);
    ArrayList<STObj> nbList = seed.getNeighbours(staticPointList.iterator(), epsilon, spatioTemp_ratio);
//    System.out.println("numOf NB: " + nbList.size());
    if (nbList.size() > minPts) {
      // stObj is a core, use an uuid as a unique new cluster ID
      // TODO: Is UUID unique enough?
      String newClusterId = UUID.randomUUID().toString(); //seed.getObjId().split("-")[1];
      seed.setClusterID(newClusterId);
      seed.setClusterLabel(STObj.LABEL_CORE);
      toBeUpdated.put(seed.getClusterID() + "#" + seed.getObjId(), seed);
      nbList.forEach(nbStObj -> {
        // if the neighbour is an un-clustered point
        if (nbStObj.getClusterID() == null || nbStObj.getClusterID().equals("")) {
          STObj duplicate = nbStObj.clone();  // TODO reconsider
          duplicate.setClusterID(newClusterId);
          duplicate.setClusterLabel(STObj.LABEL_BORDER);
          toBeUpdated.put(duplicate.getClusterID() + "#" + duplicate.getObjId(), duplicate);
          // expand - Inner self call
          extendFromSeed(nbStObj, staticPointList, toBeUpdated, epsilon, spatioTemp_ratio, minPts);  //TODO needs reconsider if this is needed or if clone is needed
        }
        // If the neighbour is a core of another cluster
        else if (nbStObj.getClusterLabel().equals(STObj.LABEL_CORE)) {
//          toBeUpdated.put(nbStObj.getClusterID() + "#" + nbStObj.getObjId(), nbStObj);
          STObj duplicate = nbStObj.clone();
          duplicate.setClusterID(newClusterId);
          // Can determine it is a border of the new cluster for now. leave merge phase to merge its real label
          duplicate.setClusterLabel(STObj.LABEL_BORDER);
          toBeUpdated.put(duplicate.getClusterID() + "#" + duplicate.getObjId(), duplicate);
        }
        // If the neighbour is a border of another cluster
        else if (nbStObj.getClusterLabel().equals(STObj.LABEL_BORDER)) {
          if (nbStObj.getNeighbours(staticPointList.iterator(), epsilon, spatioTemp_ratio).size() > minPts)
            nbStObj.setClusterLabel(STObj.LABEL_CORE);
          toBeUpdated.put(nbStObj.getClusterID() + "#" + nbStObj.getObjId(), nbStObj);
          STObj duplicate = nbStObj.clone();
          duplicate.setClusterID(newClusterId);
          toBeUpdated.put(duplicate.getClusterID() + "#" + duplicate.getObjId(), duplicate);
        }
      });
    } else {
      //flag noise
//      toBeUpdated.put("NOISE"+"#"+seed.getObjId(), seed);
    }

  }
}
