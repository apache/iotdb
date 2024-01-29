package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.thrift.TException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeTransferHandshakeV2Req extends TPipeTransferReq {
  private transient Map<String, String> params;

  private PipeTransferHandshakeV2Req() {
    // Empty constructor
  }

  public Map<String, String> getParams() {
    return params;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferHandshakeV2Req toTPipeTransferReq(HashMap<String, String> params)
      throws TException {
    final PipeTransferHandshakeV2Req handshakeReq = new PipeTransferHandshakeV2Req();

    handshakeReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    handshakeReq.type = PipeRequestType.HANDSHAKE_V2.getType();
    handshakeReq.body = ByteBuffer.wrap(SerializationUtils.serialize(params));

    return handshakeReq;
  }

  public static PipeTransferHandshakeV2Req fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferHandshakeV2Req handshakeReq = new PipeTransferHandshakeV2Req();

    handshakeReq.params = SerializationUtils.deserialize(transferReq.body.array());

    handshakeReq.version = transferReq.version;
    handshakeReq.type = transferReq.type;
    handshakeReq.body = transferReq.body;

    return handshakeReq;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTransferHandshakeBytes(HashMap<String, String> params) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(PipeRequestType.HANDSHAKE_V2.getType(), outputStream);
      ReadWriteIOUtils.write(params, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
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
    PipeTransferHandshakeV2Req that = (PipeTransferHandshakeV2Req) obj;
    return params.equals(that.params)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(params, version, type, body);
  }
}
