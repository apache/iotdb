package org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request;

import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TPipeConsensusTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeConsensusHandshakeReq extends TPipeConsensusTransferReq {
  // Use map to store params to ensure scalability
  private transient Map<String, String> params;

  public Map<String, String> getParams() {
    return params;
  }

  /////////////////////////////// Thrift ///////////////////////////////
  public final PipeConsensusHandshakeReq convertToTPipeTransferReq(Map<String, String> params)
      throws IOException {
    this.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    this.type = PipeRequestType.PIPE_CONSENSUS_HANDSHAKE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(params.size(), outputStream);
      for (final Map.Entry<String, String> entry : params.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    this.params = params;
    return this;
  }

  public final PipeConsensusHandshakeReq translateFromTPipeConsensusTransferReq(
      TPipeConsensusTransferReq transferReq) {
    Map<String, String> params = new HashMap<>();
    final int size = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < size; ++i) {
      final String key = ReadWriteIOUtils.readString(transferReq.body);
      final String value = ReadWriteIOUtils.readString(transferReq.body);
      params.put(key, value);
    }
    this.params = params;

    version = transferReq.version;
    type = transferReq.type;
    body = transferReq.body;

    return this;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeConsensusHandshakeReq that = (PipeConsensusHandshakeReq) obj;
    return Objects.equals(params, that.params)
        && version == that.version
        && type == that.type
        && Objects.equals(body, that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(params, version, type, body);
  }
}
