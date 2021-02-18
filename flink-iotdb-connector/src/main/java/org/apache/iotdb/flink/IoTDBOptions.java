/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.Serializable;
import java.util.List;

/** IoTDBOptions describes the configuration related information for IoTDB and timeseries. */
public class IoTDBOptions implements Serializable {
  private String host;
  private int port;
  private String user;
  private String password;
  private String storageGroup;
  private List<TimeseriesOption> timeseriesOptionList;

  public IoTDBOptions() {}

  public IoTDBOptions(
      String host,
      int port,
      String user,
      String password,
      String storageGroup,
      List<TimeseriesOption> timeseriesOptionList) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.storageGroup = storageGroup;
    this.timeseriesOptionList = timeseriesOptionList;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public List<TimeseriesOption> getTimeseriesOptionList() {
    return timeseriesOptionList;
  }

  public void setTimeseriesOptionList(List<TimeseriesOption> timeseriesOptionList) {
    this.timeseriesOptionList = timeseriesOptionList;
  }

  public static class TimeseriesOption implements Serializable {
    private String path;
    private TSDataType dataType = TSDataType.TEXT;
    private TSEncoding encoding = TSEncoding.PLAIN;
    private CompressionType compressor = CompressionType.SNAPPY;

    public TimeseriesOption() {}

    public TimeseriesOption(String path) {
      this.path = path;
    }

    public TimeseriesOption(
        String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
      this.path = path;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compressor = compressor;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public TSDataType getDataType() {
      return dataType;
    }

    public void setDataType(TSDataType dataType) {
      this.dataType = dataType;
    }

    public TSEncoding getEncoding() {
      return encoding;
    }

    public void setEncoding(TSEncoding encoding) {
      this.encoding = encoding;
    }

    public CompressionType getCompressor() {
      return compressor;
    }

    public void setCompressor(CompressionType compressor) {
      this.compressor = compressor;
    }
  }
}
