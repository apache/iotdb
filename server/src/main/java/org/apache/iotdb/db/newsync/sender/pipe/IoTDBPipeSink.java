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
package org.apache.iotdb.db.newsync.sender.pipe;

import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class IoTDBPipeSink implements PipeSink {
  private final PipeSink.Type type;

  private String name;
  private String ip;
  private int port;

  public IoTDBPipeSink(String name) {
    ip = SyncConstant.DEFAULT_PIPE_SINK_IP;
    port = SyncConstant.DEFAULT_PIPE_SINK_PORT;
    this.name = name;
    type = Type.IoTDB;
  }

  @Override
  public void setAttribute(String attr, String value) throws PipeSinkException {
    if (attr.equals("ip")) {
      if (!value.startsWith("'") || !value.endsWith("'"))
        throw new PipeSinkException(attr, value, TSDataType.TEXT.name());
      ip = value.substring(1, value.length() - 1);
    } else if (attr.equals("port")) {
      try {
        port = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new PipeSinkException(attr, value, TSDataType.INT32.name());
      }
    } else {
      throw new PipeSinkException(
          "There is No attribute " + attr + " in " + Type.IoTDB + " pipeSink.");
    }
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public String showAllAttributes() {
    return String.format("ip='%s',port=%d", ip, port);
  }

  @Override
  public String toString() {
    return "IoTDBPipeSink{" +
            "type=" + type +
            ", name='" + name + '\'' +
            ", ip='" + ip + '\'' +
            ", port=" + port +
            '}';
  }
}
