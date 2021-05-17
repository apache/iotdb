package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.concurrent.BlockingQueue;

public class DoubleWriteProducer {
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private int fullCnt = 0;

  public DoubleWriteProducer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue) {
    this.doubleWriteQueue = doubleWriteQueue;
  }

  public void put(Pair<DoubleWriteType, TSInsertRecordsReq> reqPair) {
    try {
      if (doubleWriteQueue.size() == 1024) {
        ++fullCnt;
      }
      doubleWriteQueue.put(reqPair);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public int getFullCnt() {
    return fullCnt;
  }
}
