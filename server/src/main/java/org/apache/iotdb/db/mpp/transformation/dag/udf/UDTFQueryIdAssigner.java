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

package org.apache.iotdb.db.mpp.transformation.dag.udf;

import java.util.concurrent.atomic.AtomicLong;

public class UDTFQueryIdAssigner {
  private final AtomicLong nextId;

  private UDTFQueryIdAssigner() {
    this.nextId = new AtomicLong();
  }

  public long getNextId() {
    return nextId.getAndIncrement();
  }

  public static UDTFQueryIdAssigner getInstance() {
    return UDTFQueryIdAssignerHolder.INSTANCE;
  }

  private static class UDTFQueryIdAssignerHolder {
    private static final UDTFQueryIdAssigner INSTANCE = new UDTFQueryIdAssigner();
  }
}
