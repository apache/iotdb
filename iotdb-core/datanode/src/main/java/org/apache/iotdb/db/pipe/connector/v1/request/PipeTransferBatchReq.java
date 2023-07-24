package org.apache.iotdb.db.pipe.connector.v1.request;

import org.apache.iotdb.db.pipe.connector.IoTDBThriftConnectorRequestVersion;
import org.apache.iotdb.db.pipe.connector.v1.PipeRequestType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeTransferBatchReq extends TPipeTransferReq {
  private PipeTransferBatchReq() {
    // do nothing
  }

  public static PipeTransferBatchReq toTPipeTransferBatchReq(List<TPipeTransferReq> reqs)
      throws IOException {
    final PipeTransferBatchReq req = new PipeTransferBatchReq();

    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    ReadWriteIOUtils.write(reqs.size(), stream);

    for (TPipeTransferReq tReq : reqs) {
      ReadWriteIOUtils.write(tReq.type, stream);
      stream.write(tReq.body.array());
      //      ReadWriteIOUtils.write(tReq.body, stream);
    }

    req.version = IoTDBThriftConnectorRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_BATCH.getType();
    req.body = ByteBuffer.wrap(stream.toByteArray());

    return req;
  }

  public static List<TPipeTransferReq> splitTPipeTransferReqs(TPipeTransferReq transferReq) {
    ArrayList<TPipeTransferReq> tPipeTransferReqs = new ArrayList<>();

    int batchSize = ReadWriteIOUtils.readInt(transferReq.body);
    for (int i = 0; i < batchSize; i++) {
      short type = ReadWriteIOUtils.readShort(transferReq.body);
      int capacity = ReadWriteIOUtils.readInt(transferReq.body);
      ByteBuffer body = ByteBuffer.wrap(ReadWriteIOUtils.readBytes(transferReq.body, capacity));

      if (type == PipeRequestType.TRANSFER_INSERT_NODE.getType()) {
        InsertNode insertNode = (InsertNode) PlanNodeType.deserialize(body);
        PipeTransferInsertNodeReq insertNodeReq = new PipeTransferInsertNodeReq(insertNode, body);
        tPipeTransferReqs.add(insertNodeReq);
      } else {
        Tablet tablet = Tablet.deserialize(body);
        PipeTransferTabletReq tabletReq = new PipeTransferTabletReq(tablet, body);
        tPipeTransferReqs.add(tabletReq);
      }
    }

    return tPipeTransferReqs;
  }
}
