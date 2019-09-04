/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.iotdb.db.tools.logvisual;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * LogEntry is a parsed log event.
 * We will take the log:
 *  "2019-08-21 09:57:16,552 [pool-4-IoTDB-Flush-ServerServiceImpl-thread-4] INFO  org.apache
 *  .iotdb.db.engine.flush.MemTableFlushTask:95 - Storage group root.perform.group_6 memtable org
 *  .apache.iotdb.db.engine.memtable.PrimitiveMemTable@66 flushing a memtable has finished! Time
 *  consumption: 9942ms"
 *  as a running example.
 */
public class LogEntry {

  // if a visualization plan has no tag, this tag will be the tag of all events
  private static List<String> DEFAULT_TAG = Collections.EMPTY_LIST;

  /**
   * The time when the log is logged.
   * "2019-08-21 09:57:16,552" in the example.
   */
  private Date date;
  /**
   * The name of the thread which has written the log.
   *  "pool-4-IoTDB-Flush-ServerServiceImpl-thread-4" in the example.
   */
  private String threadName;
  /**
   * The level of the log, one of "DEBUG", "INFO", "WARN", "ERROR".
   * "INFO" in the example.
   */
  private LogLevel logLevel;
  /**
   * The class and line of code that generated this log.
   * "org.apache.iotdb.db.engine.flush.MemTableFlushTask:95" in the example.
   */
  private CodeLocation codeLocation;
  /**
   * The message contained in the log.
   *  "Storage group root.perform.group_6 memtable org
   *  .apache.iotdb.db.engine.memtable.PrimitiveMemTable@66 flushing a memtable has finished! Time
   *  consumption: 9942ms" in the example.
   */
  private String logContent;

  /**
   * tags are a list of strings cut out of the logContent to group the logs into different groups.
   * This is specified by a given visualization plan.
   */
  private List<String> tags = DEFAULT_TAG;

  /**
   * measurements are real values occurred in the logContent that should be plotted.
   * This is also specified by a given visualization plan.
   */
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

  public enum LogLevel {
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
