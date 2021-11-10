package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.service.basic.dto.BasicResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class DataTypeUtils {
  public static TSStatus BasicRespToTSStatus(BasicResp basicResp) {
    if (basicResp == null) {
      return null;
    }
    return RpcUtils.getStatus(basicResp.getTsStatusCode(), basicResp.getMessage());
  }
}
