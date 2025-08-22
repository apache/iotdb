package org.apache.iotdb.confignode.consensus.response.service;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TGetServiceTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ServiceTableResp implements DataSet {
  private final TSStatus status;
  private final List<ServiceInformation> serviceInfoList;

  public ServiceTableResp(TSStatus status, List<ServiceInformation> serviceInfoList) {
    this.status = status;
    this.serviceInfoList = serviceInfoList;
  }

  public TGetServiceTableResp convertToThriftResponse() throws IOException {
    List<ByteBuffer> serviceTableList = new ArrayList<>();
    for (ServiceInformation serviceInfo : serviceInfoList) {
      serviceTableList.add(serviceInfo.serialize());
    }
    return new TGetServiceTableResp(status, serviceTableList);
  }

  @TestOnly
  public TSStatus getStatus() {
    return status;
  }

  @TestOnly
  public List<ServiceInformation> getServiceInfoList() {
    return serviceInfoList;
  }
}
