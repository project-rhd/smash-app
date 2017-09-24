package smash.utils.streamTasks.ml.spatioTemporal;

import smash.utils.geomesa.GeoMesaOptions;
import smash.utils.geomesa.GeoMesaWriter;

/**
 * @author Yikai Gong
 */

public class DbscanTaskOptions extends GeoMesaOptions{

  // Next cluster Id
  public Long nxtCluId;
  // Min number of points to form a cluster
  public Long minPts;
  // Time-Spatial epsilon
  public Long dist_time;
  public Double dist_spatial;
  public Double timeSpatial_ratio;
  public Double epsilon;
}
