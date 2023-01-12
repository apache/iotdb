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
package org.apache.iotdb.commons.sync.pipesink;

import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class IoTDBPipeSink implements PipeSink {
  private static final PipeSinkType pipeSinkType = PipeSinkType.IoTDB;

  private String name;
  private String ip;
  private int port;

  private static final String ATTRIBUTE_IP_KEY = "ip";
  private static final String ATTRIBUTE_PORT_KEY = "port";

  public IoTDBPipeSink() {}

  public IoTDBPipeSink(String name) {
    this();
    this.ip = SyncConstant.DEFAULT_PIPE_SINK_IP;
    this.port = SyncConstant.DEFAULT_PIPE_SINK_PORT;
    this.name = name;
  }

  @Override
  public void setAttribute(Map<String, String> params) throws PipeSinkException {
    for (Map.Entry<String, String> entry : params.entrySet()) {
      String attr = entry.getKey();
      String value = entry.getValue();

      attr = attr.toLowerCase();
      if (attr.equals(ATTRIBUTE_IP_KEY)) {
        if (!Pattern.matches(SyncConstant.IPV4_PATTERN, value)) {
          throw new PipeSinkException(
              String.format("%s is nonstandard IP address, only support IPv4 now.", value));
        }
        ip = value;
      } else if (attr.equals(ATTRIBUTE_PORT_KEY)) {
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
    return String.format("%s='%s',%s=%d", ATTRIBUTE_IP_KEY, ip, ATTRIBUTE_PORT_KEY, port);
  }

  @Override
  public TPipeSinkInfo getTPipeSinkInfo() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(ATTRIBUTE_IP_KEY, ip);
    attributes.put(ATTRIBUTE_PORT_KEY, String.valueOf(port));
    return new TPipeSinkInfo(this.name, this.pipeSinkType.name()).setAttributes(attributes);
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) pipeSinkType.ordinal(), outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(ip, outputStream);
    ReadWriteIOUtils.write(port, outputStream);
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    name = ReadWriteIOUtils.readString(inputStream);
    ip = ReadWriteIOUtils.readString(inputStream);
    port = ReadWriteIOUtils.readInt(inputStream);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    name = ReadWriteIOUtils.readString(buffer);
    ip = ReadWriteIOUtils.readString(buffer);
    port = ReadWriteIOUtils.readInt(buffer);
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
