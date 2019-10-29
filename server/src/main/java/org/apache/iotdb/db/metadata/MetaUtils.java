package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.metadata.MTree;

public class MetaUtils {
  public static String[] getNodeNames(String path, String separator) {
    String[] nodeNames;
    path = path.trim();
    if(path.contains("\"") || path.contains("\'")){
      String[] deviceAndMeasurement;
      if(path.contains("\"")){
        deviceAndMeasurement = path.split("\"");
      }else {
        deviceAndMeasurement = path.split("\'");
      }
      String device = deviceAndMeasurement[0];
      String measurement = deviceAndMeasurement[1];
      String[] deviceNodeName = device.split(separator);
      int nodeNumber = deviceNodeName.length + 1;
      nodeNames = new String[nodeNumber];
      System.arraycopy(deviceNodeName, 0, nodeNames, 0, nodeNumber - 1);
      nodeNames[nodeNumber - 1] = measurement;
    }else{
      nodeNames = path.split(separator);
    }
    return nodeNames;
  }
}
