package org.apache.iotdb.db.tools.logvisual.exceptions;

public class UnmatchedContentException extends VisualizationException {

  public UnmatchedContentException(String content, String pattern) {
    super(String.format("%s cannot match %s", content, pattern));
  }
}