package smash.app.tweets.analyzer;

import smash.data.scats.pojo.ScatsVolume;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author Yikai Gong
 */

public class AbnCounter implements Serializable{
  private ArrayList<ScatsVolume> abnList = new ArrayList<>();
  private ArrayList<ScatsVolume> normList = new ArrayList<>();

  public AbnCounter() {
  }

  public ArrayList<ScatsVolume> getAbnList() {
    return abnList;
  }

  public void setAbnList(ArrayList<ScatsVolume> abnList) {
    this.abnList = abnList;
  }

  public ArrayList<ScatsVolume> getNormList() {
    return normList;
  }

  public void setNormList(ArrayList<ScatsVolume> normList) {
    this.normList = normList;
  }

  public boolean addAbnScv(ScatsVolume acv){
    return abnList.add(acv);
  }

  public boolean addNormScv(ScatsVolume acv){
    return normList.add(acv);
  }

  public AbnCounter merge(AbnCounter target){
    this.abnList.addAll(target.getAbnList());
    this.normList.addAll(target.getNormList());
    return this;
  }
}
