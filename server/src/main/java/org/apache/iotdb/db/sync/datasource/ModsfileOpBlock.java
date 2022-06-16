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
 *
 */

package org.apache.iotdb.db.sync.datasource;

import org.apache.iotdb.db.sync.externalpipe.operation.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModsfileOpBlock extends AbstractOpBlock {
  private static final Logger logger = LoggerFactory.getLogger(ModsfileOpBlock.class);

  public ModsfileOpBlock(String sg, String modsFileName) {
    super(sg, -1);
  }

  @Override
  public long getDataCount() {
    if (dataCount >= 0) {
      return dataCount;
    }
    // ToDO:
    return 0;
  }

  @Override
  public Operation getOperation(long index, long length) {
    return null;
  }

  @Override
  public void close() {
    super.close();
  }
}
