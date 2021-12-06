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
package org.apache.iotdb.db.metadata.id_table.entry;

/** Using sha 256 hash value of device path as device ID */
public class SHA256DeviceID implements IDeviceID {
  long l1;
  long l2;
  long l3;
  long l4;

  /** The probability that each bit of sha 256 is 0 or 1 is equal */
  public int hashCode() {
    return (int) l1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SHA256DeviceID)) {
      return false;
    }
    SHA256DeviceID that = (SHA256DeviceID) o;
    return l1 == that.l1 && l2 == that.l2 && l3 == that.l3 && l4 == that.l4;
  }
}
