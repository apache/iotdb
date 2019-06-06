package org.apache.iotdb.db.cost.statistic;

public class Test {

  public static void main(String[] args) {
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH_SQL, System.nanoTime());
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH_SQL, System.nanoTime()-8000000);

    try {
      Measurement.INSTANCE.start();
      Measurement.INSTANCE.startContinuousStatistics();
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH_SQL, System.nanoTime());
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_BATCH_SQL, System.nanoTime()-8000000);
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.stopStatistic();
      Measurement.INSTANCE.stopStatistic();
      Measurement.INSTANCE.stopStatistic();
      System.out.println("After stopStatistic!");
      Thread.currentThread().sleep(1000);
      Measurement.INSTANCE.startContinuousStatistics();
      System.out.println("RE start!");
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.startContinuousStatistics();
      System.out.println("RE start2!");
      Thread.currentThread().sleep(2000);
      Measurement.INSTANCE.stopStatistic();
      System.out.println("After stopStatistic2!");
      Measurement.INSTANCE.stop();

    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
