package org.apache.iotdb.db.tools.logvisual.exceptions;

public class UnmatchedContentException extends AnalyzeException {

  public UnmatchedContentException(String content, String pattern) {
    super(String.format("%s cannot match %s", content, pattern));
  }
}