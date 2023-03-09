package org.apache.iotdb.confignode.consensus.response.pipe.plugin;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipePluginTableResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipePluginTableResp implements DataSet {
  private TSStatus status;

  private List<PipePluginInformation> allPipePluginInformation;

  public PipePluginTableResp() {}

  public PipePluginTableResp(
      TSStatus status, List<PipePluginInformation> allPipePluginInformation) {
    this.status = status;
    this.allPipePluginInformation = allPipePluginInformation;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public List<PipePluginInformation> getAllPipePluginInformation() {
    return allPipePluginInformation;
  }

  public void setAllPipePluginInformation(List<PipePluginInformation> allPipePluginInformation) {
    this.allPipePluginInformation = allPipePluginInformation;
  }

  public TGetPipePluginTableResp convertToThriftResponse() throws IOException {
    List<ByteBuffer> pipePluginInformationByteBuffers = new ArrayList<>();

    for (PipePluginInformation pipePluginInformation : allPipePluginInformation) {
      pipePluginInformationByteBuffers.add(pipePluginInformation.serialize());
    }
    return new TGetPipePluginTableResp(status, pipePluginInformationByteBuffers);
  }
}
