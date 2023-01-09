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
package org.apache.iotdb.db.engine.compaction.execute.performer.constant;

import org.apache.iotdb.db.engine.compaction.execute.performer.ISeqCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.ReadChunkCompactionPerformer;

public enum InnerSeqCompactionPerformer {
  READ_CHUNK,
  FAST;

  public static InnerSeqCompactionPerformer getInnerSeqCompactionPerformer(String name) {
    if (READ_CHUNK.toString().equalsIgnoreCase(name)) {
      return READ_CHUNK;
    } else if (FAST.toString().equalsIgnoreCase(name)) {
      return FAST;
    }
    throw new RuntimeException("Illegal compaction performer for seq inner compaction " + name);
  }

  public ISeqCompactionPerformer createInstance() {
    switch (this) {
      case READ_CHUNK:
        return new ReadChunkCompactionPerformer();
      case FAST:
        return new FastCompactionPerformer(false);
      default:
        throw new RuntimeException("Illegal compaction performer for seq inner compaction " + this);
    }
  }
}
