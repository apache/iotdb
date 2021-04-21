package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

public class OptimizeConfig {
  private double SACoolingRate = 0.01;
  private double SAInitTemperature = 100;
  private int SAMaxIteration = 20000;
  private long SAMaxTime = 30L * 60L * 1000L;
  private int recordSampleNum = 100;

  public double getSACoolingRate() {
    return SACoolingRate;
  }

  public void setSACoolingRate(double SACoolingRate) {
    this.SACoolingRate = SACoolingRate;
  }

  public double getSAInitTemperature() {
    return SAInitTemperature;
  }

  public void setSAInitTemperature(double SAInitTemperature) {
    this.SAInitTemperature = SAInitTemperature;
  }

  public int getSAMaxIteration() {
    return SAMaxIteration;
  }

  public void setSAMaxIteration(int SAMaxIteration) {
    this.SAMaxIteration = SAMaxIteration;
  }

  public long getSAMaxTime() {
    return SAMaxTime;
  }

  public void setSAMaxTime(long SAMaxTime) {
    this.SAMaxTime = SAMaxTime;
  }

  public int getRecordSampleNum() {
    return recordSampleNum;
  }

  public void setRecordSampleNum(int recordSampleNum) {
    this.recordSampleNum = recordSampleNum;
  }

  @Override
  public String toString() {
    return String.format(
        "{SACoolingRate: %f, SAInitTemperature: %f, SAMaxIteration: %d, SAMaxTime: %d ms, recordSampleNum: %d}",
        SACoolingRate, SAInitTemperature, SAMaxIteration, SAMaxTime, recordSampleNum);
  }
}
