/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.utils.cte;

import org.apache.iotdb.commons.exception.IoTDBException;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

public class MemoryReader implements CteDataReader {
  // all the data in MemoryReader lies in memory
  private final List<TsBlock> cachedData;
  private final int size;
  private int tsBlockIndex;

  public MemoryReader(List<TsBlock> cachedTsBlock) {
    this.cachedData = cachedTsBlock;
    this.size = cachedTsBlock.size();
    this.tsBlockIndex = 0;
  }

  @Override
  public boolean hasNext() throws IoTDBException {
    return cachedData != null && tsBlockIndex < size;
  }

  @Override
  public TsBlock next() throws IoTDBException {
    if (cachedData == null || tsBlockIndex >= size) {
      return null;
    }
    return cachedData.get(tsBlockIndex++);
  }

  @Override
  public void close() throws IoTDBException {}
}
