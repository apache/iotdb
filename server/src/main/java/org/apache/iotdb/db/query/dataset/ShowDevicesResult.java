/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class ShowDevicesResult extends ShowResult {
  public ShowDevicesResult() {
    super();
  }

  public ShowDevicesResult(String name, String sgName) {
    super(name, sgName);
  }

  public ShowDevicesResult(String name) {
    super(name);
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(sgName, outputStream);
  }

  public static ShowDevicesResult deserialize(ByteBuffer buffer) {
    ShowDevicesResult result = new ShowDevicesResult();
    result.name = ReadWriteIOUtils.readString(buffer);
    result.sgName = ReadWriteIOUtils.readString(buffer);
    return result;
  }

  @Override
  public String toString() {
    return "ShowDevicesResult{" + " name='" + name + '\'' + ", sgName='" + sgName + '\'' + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowDevicesResult result = (ShowDevicesResult) o;
    return Objects.equals(name, result.name) && Objects.equals(sgName, result.sgName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, sgName);
  }
}
