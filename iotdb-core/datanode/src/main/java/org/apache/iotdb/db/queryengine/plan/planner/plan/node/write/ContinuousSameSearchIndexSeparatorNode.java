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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

/**
 * For IoTConsensus sync. See <a href="https://github.com/apache/iotdb/pull/12955">github pull
 * request</a> for details.
 */
public class ContinuousSameSearchIndexSeparatorNode implements WALEntryValue {

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putShort(PlanNodeType.CONTINUOUS_SAME_SEARCH_INDEX_SEPARATOR.getNodeType());
    // search index is always -1
    buffer.putLong(-1);
  }

  @Override
  public int serializedSize() {
    return Short.BYTES + Long.BYTES;
  }
}
