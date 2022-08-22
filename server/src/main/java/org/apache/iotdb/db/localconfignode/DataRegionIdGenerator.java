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

package org.apache.iotdb.db.localconfignode;

import java.util.concurrent.atomic.AtomicInteger;

public class DataRegionIdGenerator {
  private static final DataRegionIdGenerator INSTANCE = new DataRegionIdGenerator();
  private final AtomicInteger idCounter = new AtomicInteger(0);

  public static DataRegionIdGenerator getInstance() {
    return INSTANCE;
  }

  public void setCurrentId(int id) {
    idCounter.set(id);
  }

  public int getNextId() {
    return idCounter.addAndGet(1);
  }
}
