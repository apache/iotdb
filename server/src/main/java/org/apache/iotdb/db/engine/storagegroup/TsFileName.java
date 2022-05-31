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
 */
package org.apache.iotdb.db.engine.storagegroup;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.commons.conf.IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileName implements Comparable<TsFileName> {
  /**
   * The format is {time}-{version}-{innerCompactionCnt}-{crossCompactionCnt}.tsfile. Field time and
   * version are used to guarantee the order of tsfiles, if two tsfiles have the same time, then we
   * use version to guarantee the order.
   */
  public static final Pattern TS_FILE_NAME_PATTERN =
      Pattern.compile(
          String.format(
              "(?<time>\\d+)%s(?<version>\\d+)%s(?<innerCompactionCnt>\\d+)%s(?<crossCompactionCnt>\\d+)(?<suffix>\\%s|\\%s|\\%s)$",
              FILE_NAME_SEPARATOR,
              FILE_NAME_SEPARATOR,
              FILE_NAME_SEPARATOR,
              TSFILE_SUFFIX,
              INNER_COMPACTION_TMP_FILE_SUFFIX,
              CROSS_COMPACTION_TMP_FILE_SUFFIX));

  public static final String TS_FILE_NAME_FORMAT =
      "%d" + FILE_NAME_SEPARATOR + "%d" + FILE_NAME_SEPARATOR + "%d" + FILE_NAME_SEPARATOR + "%d";

  private volatile long time;
  private volatile long version;
  private int innerCompactionCnt;
  private int crossCompactionCnt;
  private String suffix;

  public TsFileName(
      long time, long version, int innerCompactionCnt, int crossCompactionCnt, String suffix) {
    this.time = time;
    this.version = version;
    this.innerCompactionCnt = innerCompactionCnt;
    this.crossCompactionCnt = crossCompactionCnt;
    this.suffix = suffix;
  }

  public String toFileName() {
    return formatPrefix(time, version, innerCompactionCnt, crossCompactionCnt) + suffix;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public int getInnerCompactionCnt() {
    return innerCompactionCnt;
  }

  public void setInnerCompactionCnt(int innerCompactionCnt) {
    this.innerCompactionCnt = innerCompactionCnt;
  }

  public int getCrossCompactionCnt() {
    return crossCompactionCnt;
  }

  public void setCrossCompactionCnt(int crossCompactionCnt) {
    this.crossCompactionCnt = crossCompactionCnt;
  }

  private static int compare(long time1, long version1, long time2, long version2) {
    int cmp = Long.compare(time1, time2);
    if (cmp == 0) {
      cmp = Long.compare(version1, version2);
    }
    return cmp;
  }

  @Override
  public int compareTo(TsFileName other) {
    return compare(this.time, this.version, other.time, other.version);
  }

  /** Compare tsfile name, first compare createdTime, then compare versionId */
  public static int compareFileName(String tsFileName1, String tsFileName2) {
    return parse(tsFileName1).compareTo(parse(tsFileName2));
  }

  /** Compare tsfile name, first compare createdTime, then compare versionId */
  public static int compareFileName(File tsFile1, File tsFile2) {
    return compareFileName(tsFile1.getName(), tsFile2.getName());
  }

  public static int compareFileName(TsFileResource resource1, TsFileResource resource2) {
    return compare(
        resource1.getCreatedTime(),
        resource1.getVersion(),
        resource2.getCreatedTime(),
        resource2.getVersion());
  }

  private static String formatPrefix(
      long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
    return String.format(
        TS_FILE_NAME_FORMAT, time, version, innerCompactionCnt, crossCompactionCnt);
  }

  /**
   * Gets tsfile name, the format is
   * {time}-{version}-{innerCompactionCnt}-{crossCompactionCnt}.tsfile
   */
  public static String getTsFileName(
      long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
    return formatPrefix(time, version, innerCompactionCnt, crossCompactionCnt) + TSFILE_SUFFIX;
  }

  /**
   * Gets inner tsfile name, the format is
   * {time}-{version}-{innerCompactionCnt}-{crossCompactionCnt}.inner
   */
  public static String getInnerTsFileName(
      long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
    return formatPrefix(time, version, innerCompactionCnt, crossCompactionCnt)
        + INNER_COMPACTION_TMP_FILE_SUFFIX;
  }

  /**
   * Gets cross tsfile name, the format is
   * {time}-{version}-{innerCompactionCnt}-{crossCompactionCnt}.cross
   */
  public static String getCrossTsFileName(
      long time, long version, int innerCompactionCnt, int crossCompactionCnt) {
    return formatPrefix(time, version, innerCompactionCnt, crossCompactionCnt)
        + CROSS_COMPACTION_TMP_FILE_SUFFIX;
  }

  /** Judge whether the file name is valid */
  public static boolean isNameValid(String fileName) {
    return TS_FILE_NAME_PATTERN.matcher(fileName).find();
  }

  /** Parses info from tsfile name */
  public static TsFileName parse(String tsFileName) {
    Matcher matcher = TS_FILE_NAME_PATTERN.matcher(tsFileName);
    if (!matcher.find()) {
      throw new RuntimeException("TsFile file name format is incorrect: " + tsFileName);
    }
    return new TsFileName(
        Long.parseLong(matcher.group("time")),
        Long.parseLong(matcher.group("version")),
        Integer.parseInt(matcher.group("innerCompactionCnt")),
        Integer.parseInt(matcher.group("crossCompactionCnt")),
        matcher.group("suffix"));
  }

  /** Parses time from tsfile name */
  public static long parseTime(String tsFileName) {
    Matcher matcher = TS_FILE_NAME_PATTERN.matcher(tsFileName);
    if (!matcher.find()) {
      throw new RuntimeException("TsFile file name format is incorrect: " + tsFileName);
    }
    return Long.parseLong(matcher.group("time"));
  }

  /** Parses version id from tsfile name */
  public static long parseVersion(String tsFileName) {
    Matcher matcher = TS_FILE_NAME_PATTERN.matcher(tsFileName);
    if (!matcher.find()) {
      throw new RuntimeException("TsFile file name format is incorrect: " + tsFileName);
    }
    return Long.parseLong(matcher.group("version"));
  }

  /** Parses inner space merge num from tsfile name */
  public static int parseInnerCompactionCnt(String tsFileName) {
    Matcher matcher = TS_FILE_NAME_PATTERN.matcher(tsFileName);
    if (!matcher.find()) {
      throw new RuntimeException("TsFile file name format is incorrect: " + tsFileName);
    }
    return Integer.parseInt(matcher.group("innerCompactionCnt"));
  }

  /** Parses cross space merge num from tsfile name */
  public static int parseCrossCompactionCnt(String tsFileName) {
    Matcher matcher = TS_FILE_NAME_PATTERN.matcher(tsFileName);
    if (!matcher.find()) {
      throw new RuntimeException("TsFile file name format is incorrect: " + tsFileName);
    }
    return Integer.parseInt(matcher.group("crossCompactionCnt"));
  }
}
