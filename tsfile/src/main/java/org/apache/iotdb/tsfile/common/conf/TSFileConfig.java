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
package org.apache.iotdb.tsfile.common.conf;

import java.nio.charset.Charset;
import org.apache.iotdb.tsfile.fileSystem.FSType;

/**
 * TSFileConfig is a configure class. Every variables is public and has default value.
 */
public class TSFileConfig {

  // Memory configuration
  public static final int RLE_MIN_REPEATED_NUM = 8;
  public static final int RLE_MAX_REPEATED_NUM = 0x7FFF;
  public static final int RLE_MAX_BIT_PACKED_NUM = 63;

  // Data type configuration
  // Gorilla encoding configuration
  public static final int FLOAT_LENGTH = 32;
  public static final int FLAOT_LEADING_ZERO_LENGTH = 5;
  public static final int FLOAT_VALUE_LENGTH = 6;

  // Encoder configuration
  public static final int DOUBLE_LENGTH = 64;
  public static final int DOUBLE_LEADING_ZERO_LENGTH = 6;

  // RLE configuration
  public static final int DOUBLE_VALUE_LENGTH = 7;

  /**
   * String encoder with UTF-8 encodes a character to at most 4 bytes.
   */
  public static final int BYTE_SIZE_PER_CHAR = 4;
  public static final String STRING_ENCODING = "UTF-8";
  public static final Charset STRING_CHARSET = Charset.forName(STRING_ENCODING);
  public static final String CONFIG_FILE_NAME = "iotdb-engine.properties";
  public static final String MAGIC_STRING = "TsFile";
  public static final String VERSION_NUMBER = "000001";

  /**
   * The default grow size of class BatchData.
   */
  public static final int DYNAMIC_DATA_SIZE = 1000;

  public int getGroupSizeInByte() {
    return groupSizeInByte;
  }

  public void setGroupSizeInByte(int groupSizeInByte) {
    this.groupSizeInByte = groupSizeInByte;
  }

  public int getPageSizeInByte() {
    return pageSizeInByte;
  }

  public void setPageSizeInByte(int pageSizeInByte) {
    this.pageSizeInByte = pageSizeInByte;
  }

  public int getMaxNumberOfPointsInPage() {
    return maxNumberOfPointsInPage;
  }

  public void setMaxNumberOfPointsInPage(int maxNumberOfPointsInPage) {
    this.maxNumberOfPointsInPage = maxNumberOfPointsInPage;
  }

  public String getTimeSeriesDataType() {
    return timeSeriesDataType;
  }

  public void setTimeSeriesDataType(String timeSeriesDataType) {
    this.timeSeriesDataType = timeSeriesDataType;
  }

  public int getMaxStringLength() {
    return maxStringLength;
  }

  public void setMaxStringLength(int maxStringLength) {
    this.maxStringLength = maxStringLength;
  }

  public int getFloatPrecision() {
    return floatPrecision;
  }

  public void setFloatPrecision(int floatPrecision) {
    this.floatPrecision = floatPrecision;
  }

  public String getTimeEncoder() {
    return timeEncoder;
  }

  public void setTimeEncoder(String timeEncoder) {
    this.timeEncoder = timeEncoder;
  }

  public String getValueEncoder() {
    return valueEncoder;
  }

  public void setValueEncoder(String valueEncoder) {
    this.valueEncoder = valueEncoder;
  }

  public int getRleBitWidth() {
    return rleBitWidth;
  }

  public void setRleBitWidth(int rleBitWidth) {
    this.rleBitWidth = rleBitWidth;
  }

  public int getDeltaBlockSize() {
    return deltaBlockSize;
  }

  public void setDeltaBlockSize(int deltaBlockSize) {
    this.deltaBlockSize = deltaBlockSize;
  }

  public String getFreqType() {
    return freqType;
  }

  public void setFreqType(String freqType) {
    this.freqType = freqType;
  }

  public double getPlaMaxError() {
    return plaMaxError;
  }

  public void setPlaMaxError(double plaMaxError) {
    this.plaMaxError = plaMaxError;
  }

  public double getSdtMaxError() {
    return sdtMaxError;
  }

  public void setSdtMaxError(double sdtMaxError) {
    this.sdtMaxError = sdtMaxError;
  }

  public double getDftSatisfyRate() {
    return dftSatisfyRate;
  }

  public void setDftSatisfyRate(double dftSatisfyRate) {
    this.dftSatisfyRate = dftSatisfyRate;
  }

  public String getCompressor() {
    return compressor;
  }

  public void setCompressor(String compressor) {
    this.compressor = compressor;
  }

  public int getPageCheckSizeThreshold() {
    return pageCheckSizeThreshold;
  }

  public void setPageCheckSizeThreshold(int pageCheckSizeThreshold) {
    this.pageCheckSizeThreshold = pageCheckSizeThreshold;
  }

  public String getEndian() {
    return endian;
  }

  public void setEndian(String endian) {
    this.endian = endian;
  }

  /**
   * Memory size threshold for flushing to disk, default value is 128MB.
   */
  private int groupSizeInByte = 128 * 1024 * 1024;
  /**
   * The memory size for each series writer to pack page, default value is 64KB.
   */
  private int pageSizeInByte = 64 * 1024;

  // TS_2DIFF configuration
  /**
   * The maximum number of data points in a page, default value is 1024 * 1024.
   */
  private int maxNumberOfPointsInPage = 1024 * 1024;
  /**
   * Data type for input timestamp, TsFile supports INT32 or INT64.
   */
  private String timeSeriesDataType = "INT64";

  // Freq encoder configuration
  /**
   * Max length limitation of input string.
   */
  private int maxStringLength = 128;
  /**
   * Floating-point precision.
   */
  private int floatPrecision = 2;
  /**
   * Encoder of time column, TsFile supports TS_2DIFF, PLAIN and RLE(run-length encoding) Default
   * value is TS_2DIFF.
   */
  private String timeEncoder = "TS_2DIFF";
  /**
   * Encoder of value series. default value is PLAIN. For int, long data type, TsFile also supports
   * TS_2DIFF and RLE(run-length encoding). For float, double data type, TsFile also supports
   * TS_2DIFF, RLE(run-length encoding) and GORILLA. For text data type, TsFile only supports
   * PLAIN.
   */
  private String valueEncoder = "PLAIN";

  // Compression configuration
  /**
   * Default bit width of RLE encoding is 8.
   */
  private int rleBitWidth = 8;

  // Don't change the following configuration
  /**
   * Default block size of two-diff. delta encoding is 128
   */
  private int deltaBlockSize = 128;
  /**
   * Default frequency type is SINGLE_FREQ.
   */
  private String freqType = "SINGLE_FREQ";
  /**
   * Default PLA max error is 100.
   */
  private double plaMaxError = 100;
  /**
   * Default SDT max error is 100.
   */
  private double sdtMaxError = 100;
  /**
   * Default DFT satisfy rate is 0.1
   */
  private double dftSatisfyRate = 0.1;
  /**
   * Data compression method, TsFile supports UNCOMPRESSED or SNAPPY. Default value is UNCOMPRESSED
   * which means no compression
   */
  private String compressor = "UNCOMPRESSED";
  /**
   * Line count threshold for checking page memory occupied size.
   */
  private int pageCheckSizeThreshold = 100;
  /**
   * Default endian value is BIG_ENDIAN.
   */
  private String endian = "BIG_ENDIAN";

  /**
   * Default storage is in local file system
   */
  private FSType TSFileStorageFs = FSType.LOCAL;

  /**
   * Default hdfs ip is localhost
   */
  private String hdfsIp = "localhost";

  /**
   * Default hdfs port is 9000
   */
  private String hdfsPort = "9000";

  public TSFileConfig() {

  }


  public FSType getTSFileStorageFs() {
    return this.TSFileStorageFs;
  }

  public void setTSFileStorageFs(String TSFileStorageFs) {
    this.TSFileStorageFs = FSType.valueOf(TSFileStorageFs);
  }

  public String getHdfsIp() {
    return this.hdfsIp;
  }

  public void setHdfsIp(String hdfsIp) {
    this.hdfsIp = hdfsIp;
  }

  public String getHdfsPort() {
    return this.hdfsPort;
  }

  public void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }
}
