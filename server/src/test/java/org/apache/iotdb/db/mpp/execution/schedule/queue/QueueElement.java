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
package org.apache.iotdb.db.mpp.execution.schedule.queue;

public class QueueElement implements IDIndexedAccessible {
  private QueueElementID id;
  private final int value;

  public QueueElement(QueueElementID id, int value) {
    this.id = id;
    this.value = value;
  }

  public int getValue() {
    return this.value;
  }

  @Override
  public ID getDriverTaskId() {
    return id;
  }

  @Override
  public void setId(ID id) {
    this.id = (QueueElementID) id;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof QueueElement && ((QueueElement) o).getDriverTaskId().equals(this.id);
  }

  public static class QueueElementID implements ID {
    private final int id;

    public QueueElementID(int id) {
      this.id = id;
    }

    public int getId() {
      return this.id;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }

    @Override
    public String toString() {
      return String.valueOf(id);
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof QueueElementID && ((QueueElementID) o).getId() == this.id;
    }
  }
}
