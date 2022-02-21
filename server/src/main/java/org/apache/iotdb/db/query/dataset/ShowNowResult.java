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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ShowNowResult extends ShowResult {
  private String ip;
  private String systemTime;
  private String cpuLoad;
  private String totalMemorySize;
  private String freeMemorySize;

  public ShowNowResult(
      String ip, String systemTime, String cpuLoad, String totalMemorySize, String freeMemorySize) {
    this.ip = ip;
    this.systemTime = systemTime;
    this.cpuLoad = cpuLoad;
    this.totalMemorySize = totalMemorySize;
    this.freeMemorySize = freeMemorySize;
  }

  public ShowNowResult() {
    super();
  }

  public String getIp() {
    return ip;
  }

  public String getSystemTime() {
    return systemTime;
  }

  public String getCpuLoad() {
    return cpuLoad;
  }

  public String getTotalMemorySize() {
    return totalMemorySize;
  }

  public String getFreeMemorySize() {
    return freeMemorySize;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public void setSystemTime(String systemTime) {
    this.systemTime = systemTime;
  }

  public void setCpuLoad(String cpuLoad) {
    this.cpuLoad = cpuLoad;
  }

  public void setTotalMemorySize(String totalMemorySize) {
    this.totalMemorySize = totalMemorySize;
  }

  public void setFreeMemorySize(String freeMemorySize) {
    this.freeMemorySize = freeMemorySize;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(ip, outputStream);
    ReadWriteIOUtils.write(systemTime, outputStream);
    ReadWriteIOUtils.write(cpuLoad, outputStream);
    ReadWriteIOUtils.write(totalMemorySize, outputStream);
    ReadWriteIOUtils.write(freeMemorySize, outputStream);
  }

  public static ShowNowResult deserialize(ByteBuffer buffer) {
    ShowNowResult result = new ShowNowResult();

    result.ip = ReadWriteIOUtils.readString(buffer);
    result.systemTime = ReadWriteIOUtils.readString(buffer);
    result.cpuLoad = ReadWriteIOUtils.readString(buffer);
    result.totalMemorySize = ReadWriteIOUtils.readString(buffer);
    result.freeMemorySize = ReadWriteIOUtils.readString(buffer);
    return result;
  }

  @Override
  public String toString() {
    return "ShowNowResult{"
        + "ip='"
        + ip
        + '\''
        + ", systemTime='"
        + systemTime
        + '\''
        + ", cpuLoad='"
        + cpuLoad
        + '\''
        + ", totalMemorySize='"
        + totalMemorySize
        + '\''
        + ", freeMemorySize='"
        + freeMemorySize
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowNowResult that = (ShowNowResult) o;
    return Objects.equals(ip, that.ip)
        && Objects.equals(systemTime, that.systemTime)
        && Objects.equals(cpuLoad, that.cpuLoad)
        && Objects.equals(totalMemorySize, that.totalMemorySize)
        && Objects.equals(freeMemorySize, that.freeMemorySize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, systemTime, cpuLoad, totalMemorySize, freeMemorySize);
  }

  @Override
  public int compareTo(ShowResult o) {
    ShowNowResult result = (ShowNowResult) o;
    if (ip.compareTo(result.getIp()) != 0) {
      return ip.compareTo(result.getIp());
    }
    return 0;
  }
}
