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
package org.apache.iotdb.db.nvm.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public class SpaceStatusBitMap extends NVMSpaceMetadata {

  public SpaceStatusBitMap() throws IOException {
  }

  public void setUse(int index, boolean isTime) {
    space.put(index, isTime ? (byte) 1 : (byte) 2);
  }

  public void setFree(int index) {
    space.put(index, (byte) 0);
  }

  public List<Integer> getValidTimeSpaceIndexList(int count) {
    List<Integer> validTimeSpaceIndexList = new ArrayList<>();
    for (int i = 0; i < space.getSize(); i++) {
      byte flag = space.get(i);
      if (flag == 1) {
        validTimeSpaceIndexList.add(i);
        if (validTimeSpaceIndexList.size() == count) {
          break;
        }
      }
    }
    return validTimeSpaceIndexList;
  }

  @Override
  int getUnitSize() {
    return Byte.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
