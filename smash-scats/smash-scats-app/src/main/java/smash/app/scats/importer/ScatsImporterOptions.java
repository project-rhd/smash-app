package smash.app.scats.importer;

import org.kohsuke.args4j.Option;
import smash.utils.geomesa.GeoMesaOptions;

/**
 * @author Yikai Gong
 */

public class ScatsImporterOptions extends GeoMesaOptions {

  @Option(name = "--inputVolumeCSV", required = true,
    usage = "path to raw volume data for processing")
  public String inputVolumeCSV;

  @Option(name = "--inputLayoutCSV", required = true,
    usage = "path to site layout data for processing")
  public String inputLayoutCSV;

  @Option(name = "--inputPointShapeFile", required = true,
    usage = "path to points (traffic lights) shape file for processing")
  public String inputPointShapeFile;

  @Option(name = "--inputLineShapeFile", required = true,
    usage = "path to lines (road network) shape file for processing")
  public String inputLineShapeFile;

}
