package smash.stream.tweets;

import org.kohsuke.args4j.Option;
import smash.utils.geomesa.GeoMesaOptions;

/**
 * @author Yikai Gong
 */

public class StreamAppOptions extends GeoMesaOptions{

  @Option(name = "--intervalSec", required = true,
    usage = "interval in sec between the start of each micro-batch task")
  public Long intervalSec;

  @Option(name = "--maxPts", required = true,
    usage = "Maximum number of points for FC partition")
  public Long maxPts;
}
