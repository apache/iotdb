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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.constant;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.IllegalCompactionPerformerException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.IUnseqCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;

import org.apache.tsfile.encrypt.EncryptParameter;

public enum InnerUnseqCompactionPerformer {
  READ_POINT,
  FAST;

  public static InnerUnseqCompactionPerformer getInnerUnseqCompactionPerformer(String name) {
    if (READ_POINT.toString().equalsIgnoreCase(name)) {
      return READ_POINT;
    } else if (FAST.toString().equalsIgnoreCase(name)) {
      return FAST;
    }
    throw new IllegalCompactionPerformerException(
        "Illegal compaction performer for unseq inner compaction " + name);
  }

  @TestOnly
  public IUnseqCompactionPerformer createInstance() {
    switch (this) {
      case READ_POINT:
        return new ReadPointCompactionPerformer();
      case FAST:
        return new FastCompactionPerformer(false);
      default:
        throw new IllegalCompactionPerformerException(
            "Illegal compaction performer for unseq inner compaction " + this);
    }
  }

  public IUnseqCompactionPerformer createInstance(EncryptParameter encryptParameter) {
    switch (this) {
      case READ_POINT:
        return new ReadPointCompactionPerformer(encryptParameter);
      case FAST:
        return new FastCompactionPerformer(false, encryptParameter);
      default:
        throw new IllegalCompactionPerformerException(
            "Illegal compaction performer for unseq inner compaction " + this);
    }
  }
}
