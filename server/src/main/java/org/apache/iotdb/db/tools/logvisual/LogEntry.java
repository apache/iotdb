package org.apache.iotdb.db.tools.logvisual;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class LogEntry {

  private static List<String> DEFAULT_TAG = Collections.EMPTY_LIST;

  private Date date;
  private String threadName;
  private LogLevel logLevel;
  private CodeLocation codeLocation;
  private String logContent;

  private List<String> tags = DEFAULT_TAG;
  private List<Double> measurements;

  LogEntry(Date date, String threadName,
      LogLevel logLevel, CodeLocation codeLocation, String logContent) {
    this.date = date;
    this.threadName = threadName;
    this.logLevel = logLevel;
    this.codeLocation = codeLocation;
    this.logContent = logContent;
  }

  public Date getDate() {
    return date;
  }

  public String getThreadName() {
    return threadName;
  }

  public LogLevel getLogLevel() {
    return logLevel;
  }
  public CodeLocation getCodeLocation() {
    return codeLocation;
  }

  public String getLogContent() {
    return logContent;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public List<Double> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<Double> measurements) {
    this.measurements = measurements;
  }

  enum LogLevel {
    DEBUG, INFO, WARN, ERROR
  }

  static class CodeLocation {
    private String className;
    private int lineNum;

    CodeLocation(String className, int lineNum) {
      this.className = className;
      this.lineNum = lineNum;
    }

    public String getClassName() {
      return className;
    }

    public int getLineNum() {
      return lineNum;
    }
  }
}
