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
package org.apache.iotdb.db.nvm.space;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMStringBuffer {

  private List<String> existStringList;

  private long size;
  private NVMSpace count;
  private NVMSpace lens;
  private NVMSpace values;

  public NVMStringBuffer(long size) throws IOException {
    this.size = size;

    NVMSpaceManager spaceManager = NVMSpaceManager.getInstance();
    count = spaceManager.allocateSpace(NVMSpaceManager.getPrimitiveTypeByteSize(TSDataType.INT32));
    lens = spaceManager.allocateSpace(size / 2);
    values = spaceManager.allocateSpace(size / 2);

    recover();
  }

  private void recover() {
    int stringListLen = count.getInt(0);
    existStringList = new ArrayList<>(stringListLen);
    for (int i = 0; i < stringListLen; i++) {
      int stringLen = lens.getInt(i);
      byte[] bytes = new byte[stringLen];
      values.get(bytes);
      existStringList.add(new String(bytes));
    }
  }

  public int put(String s) {
    if (existStringList.contains(s)) {
      return existStringList.indexOf(s);
    } else {
      existStringList.add(s);
      serialize(s);
      return existStringList.size() - 1;
    }
  }

  private void serialize(String s) {
    count.putInt(0, existStringList.size());
    lens.putInt(existStringList.size() - 1, s.length());
    values.put(s.getBytes());
  }

  public String get(int index) {
    return existStringList.get(index);
  }
}
