/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.mpp.common.InstanceId;

public class InstanceContext {

  private InstanceId id;

  private final long createNanos = System.nanoTime();

  //    private final GcMonitor gcMonitor;
  //    private final AtomicLong startNanos = new AtomicLong();
  //    private final AtomicLong startFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong startFullGcTimeNanos = new AtomicLong(-1);
  //    private final AtomicLong endNanos = new AtomicLong();
  //    private final AtomicLong endFullGcCount = new AtomicLong(-1);
  //    private final AtomicLong endFullGcTimeNanos = new AtomicLong(-1);

  public InstanceContext(InstanceId id) {
    this.id = id;
  }
}
