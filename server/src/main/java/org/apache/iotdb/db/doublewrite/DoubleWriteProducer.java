package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.concurrent.BlockingQueue;

public class DoubleWriteProducer {
  private BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue;
  private int fullCnt = 0;
  private long produceCnt = 0;
  private long produceTime = 0;

  public DoubleWriteProducer(
      BlockingQueue<Pair<DoubleWriteType, TSInsertRecordsReq>> doubleWriteQueue) {
    this.doubleWriteQueue = doubleWriteQueue;
  }

  public void put(Pair<DoubleWriteType, TSInsertRecordsReq> reqPair) {
    try {
      long startTime = System.currentTimeMillis();
      if (doubleWriteQueue.size() == 1024) {
        ++fullCnt;
      }
      produceCnt += 1;
      doubleWriteQueue.put(reqPair);
      long endTime = System.currentTimeMillis();
      produceTime += endTime - startTime;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public int getFullCnt() {
    return fullCnt;
  }

  public double getEfficiency() {
    return (double) produceCnt / (double) produceTime * 1000.0;
  }
}
