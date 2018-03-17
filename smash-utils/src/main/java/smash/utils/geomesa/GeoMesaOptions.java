package smash.utils.geomesa;

import org.apache.commons.beanutils.BeanUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.BooleanOptionHandler;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Yikai Gong
 */

public class GeoMesaOptions implements Serializable {

  // Options definitions
  @Option(name = "--instanceId", required = true, usage = "the ID (name) of the Accumulo instance, e.g:  mycloud")
  public String instanceId;

  @Option(name = "--zookeepers", required = true, usage = "the comma-separated list of Zookeeper nodes that support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181")
  public String zookeepers;

  @Option(name = "--user", required = true, usage = "he Accumulo user that will own the connection, e.g.:  root")
  public String user;

  @Option(name = "--password", required = true, usage = "the password for the Accumulo user that will own the connection, e.g.:  thor")
  public String password;

  @Option(name = "--auths", required = false, usage = "the (optional) list of comma-separated Accumulo authorizations that should be applied to all data written or read by this Accumulo user; note that this is NOT the list of low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell data, e.g.:  Accounting,Purchasing,Testing")
  public String auths;

  @Option(name = "--tableName", required = true, usage = "the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data")
  public String tableName;

  @Option(name = "--overwrite", required = false, handler = BooleanOptionHandler.class, usage = "use this option if you want to overwrite the existing data in the table, default is true")
  public boolean overwrite;

  public void parse(String[] args) throws CmdLineException,
    NoSuchFieldException, SecurityException, IllegalArgumentException,
    IllegalAccessException {
    CmdLineParser parser = new CmdLineParser(this);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      throw e;
    }
  }

  public Map<String, String> getAccumuloOptions() {
    Map<String, String> values = new HashMap<>();
    values.put("instanceId", this.instanceId);
    values.put("zookeepers", this.zookeepers);
    values.put("user", this.user);
    values.put("password", this.password);
    values.put("tableName", this.tableName);
    if (this.auths != null)
      values.put("auths", this.auths);
//    values.put("generateStats", Boolean.FALSE.toString());
    return values;
  }

  public Map<String, Serializable> getAccumuloOptions2() {
    Map<String, String> values = getAccumuloOptions();
    Map<String, Serializable> results = new HashMap<>();
    values.forEach(results::put);
    return results;
  }

  public GeoMesaOptions copy(){
    GeoMesaOptions copy = new GeoMesaOptions();
    try {
      BeanUtils.copyProperties(copy, this);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    return copy;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getZookeepers() {
    return zookeepers;
  }

  public void setZookeepers(String zookeepers) {
    this.zookeepers = zookeepers;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getAuths() {
    return auths;
  }

  public void setAuths(String auths) {
    this.auths = auths;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }
}
