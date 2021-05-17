package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.BlockingQueue;

public class DoubleWriteConsumer implements Runnable {
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private TSIService.Iface doubleWriteClient;
  private TTransport transport;
  private long sessionId;
  private long consumerCnt = 0;
  private long consumerTime = 0;

  public DoubleWriteConsumer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue,
      TSIService.Iface doubleWriteClient,
      TTransport transport,
      long sessionId) {
    this.doubleWriteQueue = doubleWriteQueue;
    this.doubleWriteClient = doubleWriteClient;
    this.transport = transport;
    this.sessionId = sessionId;
  }

  @Override
  public void run() {
    try {
      while (true) {
        long startTime = System.nanoTime();
        Pair<DoubleWriteType, TSInsertRecordsReq> head = doubleWriteQueue.take();
        if (head.left == DoubleWriteType.DOUBLE_WRITE_END) {
          break;
        }
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

            doubleWriteClient.insertRecords(tsInsertRecordsReq);
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
        consumerCnt += 1;
        long endTime = System.nanoTime();
        consumerTime += endTime - startTime;
      }

      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      try {
        doubleWriteClient.closeSession(req);
      } catch (TException e) {
        throw new IoTDBConnectionException(
            "Error occurs when closing session at server. Maybe server is down.", e);
      } finally {
        if (transport != null) {
          transport.close();
        }
      }
    } catch (TException | InterruptedException | IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  public double getEfficiency() {
    return (double) consumerCnt / (double) consumerTime * 1000000000.0;
  }
}
