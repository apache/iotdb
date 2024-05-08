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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;

import java.util.concurrent.atomic.AtomicBoolean;

public class DriverContext {

  private final FragmentInstanceContext fragmentInstanceContext;

  private final AtomicBoolean finished = new AtomicBoolean();

  public DriverContext(FragmentInstanceContext fragmentInstanceContext) {
    this.fragmentInstanceContext = fragmentInstanceContext;
  }

  public FragmentInstanceId getId() {
    return fragmentInstanceContext.getId();
  }

  public FragmentInstanceContext getFragmentInstanceContext() {
    return fragmentInstanceContext;
  }

  public void failed(Throwable cause) {
    fragmentInstanceContext.failed(cause);
    finished.set(true);
  }

  public void finished() {
    finished.compareAndSet(false, true);
  }

  public boolean isDone() {
    return finished.get();
  }
}
