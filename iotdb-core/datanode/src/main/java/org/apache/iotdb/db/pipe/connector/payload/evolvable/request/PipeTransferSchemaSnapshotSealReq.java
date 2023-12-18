package org.apache.iotdb.db.pipe.connector.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.connector.payload.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.connector.payload.request.PipeTransferFileSealReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferSchemaSnapshotSealReq extends PipeTransferFileSealReq {
  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_SCHEMA_SNAPSHOT_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferSchemaSnapshotSealReq toTPipeTransferReq(
      String fileName, long fileLength) throws IOException {
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq().convertToTPipeTransferReq(fileName, fileLength);
  }

  public static PipeTransferSchemaSnapshotSealReq fromTPipeTransferReq(TPipeTransferReq req) {
    return (PipeTransferSchemaSnapshotSealReq)
        new PipeTransferSchemaSnapshotSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferFileSealBytes(String fileName, long fileLength)
      throws IOException {
    return new PipeTransferSchemaSnapshotSealReq()
        .convertToTPipeTransferSnapshotSealBytes(fileName, fileLength);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferSchemaSnapshotSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
