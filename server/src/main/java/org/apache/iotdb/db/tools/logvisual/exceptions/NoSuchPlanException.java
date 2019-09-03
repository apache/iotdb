package org.apache.iotdb.db.tools.logvisual.exceptions;

public class NoSuchPlanException extends VisualizeException {

  public NoSuchPlanException(String planName) {
    super(String.format("No such plan %s", planName));
  }
}