package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

public class OptimizeConfig {
  private double SACoolingRate = 0.01;
  private double SAInitTemperature = 100;
  private int SAMaxIteration = 2000;
  private long SAMaxTime = 30L * 60L * 1000L;
  private int recordSampleNum = 100;
  private int logEpoch = 100;
  private boolean verbose = true;

  public OptimizeConfig() {}

  public OptimizeConfig(
      double SACoolingRate,
      double SAInitTemperature,
      int SAMaxIteration,
      long SAMaxTime,
      int recordSampleNum) {
    this.SACoolingRate = SACoolingRate;
    this.SAInitTemperature = SAInitTemperature;
    this.SAMaxIteration = SAMaxIteration;
    this.SAMaxTime = SAMaxTime;
    this.recordSampleNum = recordSampleNum;
  }

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

  public int getLogEpoch() {
    return logEpoch;
  }

  public void setLogEpoch(int logEpoch) {
    this.logEpoch = logEpoch;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  @Override
  public String toString() {
    return String.format(
        "{SACoolingRate: %f, SAInitTemperature: %f, SAMaxIteration: %d, SAMaxTime: %d ms, recordSampleNum: %d}",
        SACoolingRate, SAInitTemperature, SAMaxIteration, SAMaxTime, recordSampleNum);
  }
}
