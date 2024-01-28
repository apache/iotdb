package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.iotdb.commons.pipe.connector.payload.request.IoTDBConnectorRequestVersion;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.thrift.TException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class PipeTransferHandshakeReqV2 extends TPipeTransferReq {
    private transient Map<String, String> params;

    private PipeTransferHandshakeReqV2() {
        // Empty constructor
    }

    public Map<String, String> getParams() {
        return params;
    }

    /////////////////////////////// Thrift ///////////////////////////////

    public static PipeTransferHandshakeReqV2 toTPipeTransferReq(HashMap<String, String> params) throws TException {
        final PipeTransferHandshakeReqV2 handshakeReq = new PipeTransferHandshakeReqV2();

        handshakeReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
        handshakeReq.type = PipeRequestType.HANDSHAKE.getType();
        handshakeReq.body = ByteBuffer.wrap(SerializationUtils.serialize(params));

        return handshakeReq;
    }

    public static PipeTransferHandshakeReqV2 fromTPipeTransferReq(TPipeTransferReq transferReq) {
        final PipeTransferHandshakeReqV2 handshakeReq = new PipeTransferHandshakeReqV2();

        handshakeReq.params = SerializationUtils.deserialize(transferReq.body.array());

        handshakeReq.version = transferReq.version;
        handshakeReq.type = transferReq.version;
        handshakeReq.body = transferReq.body;

        return handshakeReq;
    }

    /////////////////////////////// Air Gap ///////////////////////////////

    public static byte[] toTransferHandshakeBytes(HashMap<String, String> params) throws IOException {
        try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
             final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
            ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
            ReadWriteIOUtils.write(PipeRequestType.HANDSHAKE.getType(), outputStream);
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
        PipeTransferHandshakeReqV2 that = (PipeTransferHandshakeReqV2) obj;
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
