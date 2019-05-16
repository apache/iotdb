package org.apache.iotdb.cluster.query.utils;

import org.apache.iotdb.cluster.query.manager.common.FillBatchData;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class ClusterTimeValuePairUtils {

  private ClusterTimeValuePairUtils() {
  }

  /**
   * get given data's current (time,value) pair.
   *
   * @param data -batch data
   * @return -given data's (time,value) pair
   */
  public static TimeValuePair getCurrentTimeValuePair(BatchData data) {
    if (data instanceof FillBatchData){
      return ((FillBatchData)data).getTimeValuePair();
    }else{
      return TimeValuePairUtils.getCurrentTimeValuePair(data);
    }
  }
}
