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
package org.apache.iotdb.db.engine.compaction.constant;

import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.performer.impl.ReadPointCompactionPerformer;

public enum CrossCompactionPerformer {
  READ_POINT;

  public static CrossCompactionPerformer getCrossCompactionPerformer(String name) {
    if (READ_POINT.toString().equalsIgnoreCase(name)) {
      return READ_POINT;
    }
    throw new RuntimeException("Illegal compaction performer for cross compaction " + name);
  }

  public ICrossCompactionPerformer getCompactionPerformer() {
    switch (this) {
      case READ_POINT:
      default:
        return new ReadPointCompactionPerformer();
    }
  }
}
