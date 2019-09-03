package org.apache.iotdb.db.tools.logvisual.exceptions;

public class NoSuchPlanException extends VisualizationException {

  public NoSuchPlanException(String planName) {
    super(String.format("No such plan %s", planName));
  }
}