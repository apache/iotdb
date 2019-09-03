package org.apache.iotdb.db.tools.logvisual.exceptions;

public class NoLogFileLoadedException extends VisualizationException {

  public NoLogFileLoadedException() {
    super("No log file is loaded, please load a log file first");
  }
}