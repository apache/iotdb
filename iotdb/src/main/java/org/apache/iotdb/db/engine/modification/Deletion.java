/**
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

package org.apache.iotdb.db.engine.modification;

import java.util.Objects;

/**
 * Deletion is a delete operation on a timeseries.
 */
public class Deletion extends Modification {
  private long timestamp;

  public Deletion(String path, long versionNum, long timestamp) {
    super(Type.DELETION, path, versionNum);
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Modification))
      return false;
    Deletion del = (Deletion) obj;
    return super.equals(obj) && del.timestamp == this.timestamp;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), timestamp);
  }
}
