package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.util.ArrayList;
import java.util.List;

public class DsTypeEncodeModel {

  private String typeName;

  private List<String> encodeNameList = new ArrayList<>();

  private List<Encoder> encoders = new ArrayList<>();

  private List<PublicBAOS> publicBAOS = new ArrayList<>();

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public List<String> getEncodeNameList() {
    return encodeNameList;
  }

  public void setEncodeNameList(List<String> encodeNameList) {
    this.encodeNameList = encodeNameList;
  }

  public List<Encoder> getEncoders() {
    return encoders;
  }

  public void setEncoders(List<Encoder> encoders) {
    this.encoders = encoders;
  }

  public List<PublicBAOS> getPublicBAOS() {
    return publicBAOS;
  }

  public void setPublicBAOS(List<PublicBAOS> publicBAOS) {
    this.publicBAOS = publicBAOS;
  }
}
