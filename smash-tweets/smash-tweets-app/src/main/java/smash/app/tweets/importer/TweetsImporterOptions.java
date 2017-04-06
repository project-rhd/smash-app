package smash.app.tweets.importer;

import org.kohsuke.args4j.Option;
import smash.utils.geomesa.GeoMesaOptions;

/**
 * @author Yikai Gong
 */

public class TweetsImporterOptions extends GeoMesaOptions {

  // Options definitions
  @Option(name = "--inputFile", required = true, usage = "the HDFS file holding the tweet to process")
  public String inputFile;

  @Option(name = "--dictionaryFile", required = true, usage = "the output HDFS file holding the dictionary extracted from the Tweets")
  public String dictionaryFile;

  @Option(name = "--master", required = false, usage = "Spark server name and address")
  public String master;

  @Option(name = "--name", required = false, usage = "Job name")
  public String name;
}
