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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

// For degrade
public class UpdateContainerStatistics {
  private static final long MIN_DEGRADE_MEMORY =
      IoTDBDescriptor.getInstance().getConfig().getDetailContainerMinDegradeMemoryInBytes();
  private long lastUpdateTime = System.currentTimeMillis();
  private long size = 0;

  long getSize() {
    return size;
  }

  void addSize(final long increment) {
    this.size += increment;
  }

  void decreaseSize(final long decrement) {
    this.size -= decrement;
    lastUpdateTime = System.currentTimeMillis();
  }

  boolean needDegrade() {
    return size >= MIN_DEGRADE_MEMORY;
  }

  long getDegradePriority() {
    return size * (System.currentTimeMillis() - lastUpdateTime);
  }
}
