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
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;

public class OffsetMemo extends NVMSpaceMetadata {

  public OffsetMemo() throws IOException {
  }

  public void set(int index, long offset) {
    space.putLong(index, offset);
  }

  public long get(int index) {
    return space.getLong(index);
  }

  @Override
  int getUnitSize() {
    return Long.BYTES;
  }

  @Override
  int getUnitNum() {
    return NVMSpaceManager.NVMSPACE_NUM_MAX;
  }
}
