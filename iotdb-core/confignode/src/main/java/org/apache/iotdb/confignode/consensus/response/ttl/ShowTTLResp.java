package org.apache.iotdb.confignode.consensus.response.ttl;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TShowTTLResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Map;

public class ShowTTLResp implements DataSet {
  private TSStatus status;

  private Map<String, Long> pathTTLMap;

  public ShowTTLResp() {
    // empty constructor
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public Map<String, Long> getPathTTLMap() {
    return pathTTLMap;
  }

  public void setPathTTLMap(Map<String, Long> pathTTLMap) {
    this.pathTTLMap = pathTTLMap;
  }

  public TShowTTLResp convertToRPCTShowTTLResp() {
    TShowTTLResp tShowTTLResp = new TShowTTLResp();
    tShowTTLResp.setStatus(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      tShowTTLResp.setPathTTLMap(pathTTLMap);
    }
    return tShowTTLResp;
  }
}
