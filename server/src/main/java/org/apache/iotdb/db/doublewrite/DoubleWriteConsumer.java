package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.concurrent.BlockingQueue;

public class DoubleWriteConsumer implements Runnable {
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private Session doubleWriteSession;

  public DoubleWriteConsumer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue,
      Session doubleWriteSession) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.doubleWriteSession = doubleWriteSession;
  }

  @Override
  public void run() {
    try {
      Pair<DoubleWriteType, TSInsertRecordsReq> head;
      while (!((head = doubleWriteQueue.take()).left == DoubleWriteType.DOUBLE_WRITE_END)) {
        switch (head.left) {
            //          case TSInsertRecordReq:
            //            InsertRowPlan plan = new InsertRowPlan();
            //            plan.deserialize(head.right);
            //            TSInsertRecordReq req = new TSInsertRecordReq();
            //            req.setTimestamp(plan.getTime());
            //            req.setMeasurements(Arrays.asList(plan.getMeasurements()));
            //            req.setValues(plan.getValues());
            //            //            doubleWriteSession.insertRecord(
            //            //                tsInsertRecordReq.getDeviceId(), (TSInsertRecordReq)
            // head.right
            //            //            );
            //            doubleWriteSession.insertRecord("root.dw.d02", (TSInsertRecordReq)
            // head.right);
            //            break;
          case TSInsertRecordsReq:
            TSInsertRecordsReq tsInsertRecordsReq = head.right;

            // for test
            //            List<String> deviceIds = new ArrayList<>();
            //            for (int i = 0; i < tsInsertRecordsReq.getDeviceIds().size(); i++) {
            //              deviceIds.add("root.wd.d02");
            //            }
            //            tsInsertRecordsReq.setDeviceIds(deviceIds);
            //            System.out.println(tsInsertRecordsReq.getDeviceIds());

            doubleWriteSession.insertRecords(tsInsertRecordsReq);
            break;
            //          case TSInsertRecordsOfOneDeviceReq:
            //            TSInsertRecordsOfOneDeviceReq tsInsertRecordsOfOneDeviceReq =
            //                (TSInsertRecordsOfOneDeviceReq) head.right;
            //            doubleWriteSession.insertRecordsOfOneDevice(
            //                tsInsertRecordsOfOneDeviceReq.getDeviceId(),
            // tsInsertRecordsOfOneDeviceReq);
            //            break;
            //          case TSInsertStringRecordsReq:
            //            TSInsertStringRecordsReq tsInsertStringRecordsReq =
            //                (TSInsertStringRecordsReq) head.right;
            //            doubleWriteSession.insertRecords(
            //                tsInsertStringRecordsReq.getDeviceIds(),
            //                tsInsertStringRecordsReq.getTimestamps(),
            //                tsInsertStringRecordsReq.getMeasurementsList(),
            //                tsInsertStringRecordsReq.getValuesList());
            //            break;
        }
      }
    } catch (InterruptedException | IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }
}
