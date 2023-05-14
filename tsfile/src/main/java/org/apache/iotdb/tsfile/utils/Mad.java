package org.apache.iotdb.tsfile.utils;

import java.io.Serializable;

public class Mad implements Serializable {
  public double result;
  public double error_bound;

  public Mad(double result, double error_bound) {
    this.result = result;
    this.error_bound = error_bound;
  }

  public String toString() {
    return result + " - " + error_bound;
  }

  public double getResult() {
    return result;
  }

  public void setResult(double result) {
    this.result = result;
  }

  public double getError_bound() {
    return error_bound;
  }

  public void setError_bound(double error_bound) {
    this.error_bound = error_bound;
  }
}
