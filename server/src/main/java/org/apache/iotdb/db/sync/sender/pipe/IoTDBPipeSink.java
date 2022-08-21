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
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.Objects;

public class IoTDBPipeSink implements PipeSink {
  private final PipeSinkType pipeSinkType;

  private String name;
  private String ip;
  private int port;

  public IoTDBPipeSink(String name) {
    ip = SyncConstant.DEFAULT_PIPE_SINK_IP;
    port = SyncConstant.DEFAULT_PIPE_SINK_PORT;
    this.name = name;
    pipeSinkType = PipeSinkType.IoTDB;
  }

  @Override
  public void setAttribute(List<Pair<String, String>> params) throws PipeSinkException {
    for (Pair<String, String> pair : params) {
      String attr = pair.left;
      String value = pair.right;

      attr = attr.toLowerCase();
      if (attr.equals("ip")) {
        ip = value;
      } else if (attr.equals("port")) {
        try {
          port = Integer.parseInt(value);
        } catch (NumberFormatException e) {
          throw new PipeSinkException(attr, value, TSDataType.INT32.name());
        }
      } else {
        throw new PipeSinkException(
            "There is No attribute " + attr + " in " + PipeSinkType.IoTDB + " pipeSink.");
      }
    }
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String getPipeSinkName() {
    return name;
  }

  @Override
  public PipeSinkType getType() {
    return pipeSinkType;
  }

  @Override
  public String showAllAttributes() {
    return String.format("ip='%s',port=%d", ip, port);
  }

  @Override
  public String toString() {
    return "IoTDBPipeSink{"
        + "pipeSinkType="
        + pipeSinkType
        + ", name='"
        + name
        + '\''
        + ", ip='"
        + ip
        + '\''
        + ", port="
        + port
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IoTDBPipeSink pipeSink = (IoTDBPipeSink) o;
    return port == pipeSink.port
        && pipeSinkType == pipeSink.pipeSinkType
        && Objects.equals(name, pipeSink.name)
        && Objects.equals(ip, pipeSink.ip);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeSinkType, name, ip, port);
  }
}
