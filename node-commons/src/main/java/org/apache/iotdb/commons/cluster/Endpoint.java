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

package org.apache.iotdb.commons.cluster;

import java.nio.ByteBuffer;
import java.util.Objects;

// TODO Use a mature IDL framework such as Protobuf to manage this structure
public class Endpoint {

  private String ip;
  private int port;

  public Endpoint() {}

  public Endpoint(String ip, int port) {
    this.ip = ip;
    this.port = port;
  }

  public String getIp() {
    return ip;
  }

  public Endpoint setIp(String ip) {
    this.ip = ip;
    return this;
  }

  public int getPort() {
    return port;
  }

  public Endpoint setPort(int port) {
    this.port = port;
    return this;
  }

  public void serializeImpl(ByteBuffer buffer) {
    byte[] bytes = ip.getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);

    buffer.putInt(port);
  }

  public void deserializeImpl(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes, 0, length);
    ip = new String(bytes, 0, length);

    port = buffer.getInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Endpoint endpoint = (Endpoint) o;
    return port == endpoint.port && Objects.equals(ip, endpoint.ip);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ip, port);
  }

  @Override
  public String toString() {
    return String.format("%s:%d", ip, port);
  }
}
