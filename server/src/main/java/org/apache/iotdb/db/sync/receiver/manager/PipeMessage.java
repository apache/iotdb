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
package org.apache.iotdb.db.sync.receiver.manager;

import java.util.Objects;

public class PipeMessage {
  private MsgType type;
  private String msg;

  public PipeMessage(MsgType msgType, String msg) {
    this.type = msgType;
    this.msg = msg;
  }

  public MsgType getType() {
    return type;
  }

  public void setType(MsgType type) {
    this.type = type;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipeMessage that = (PipeMessage) o;
    return type == that.type && Objects.equals(msg, that.msg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, msg);
  }

  public enum MsgType {
    INFO(1),
    WARN(2),
    ERROR(3);

    private int value;

    MsgType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }
}
