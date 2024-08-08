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
package org.apache.iotdb.tsfile.read.common;

/** It is an empty signal to notify the caller that there is no more batch data after it. */
public class SignalBatchData extends BatchData {

  private static final long serialVersionUID = -4175548102820374070L;

  public static SignalBatchData getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      // allowed to do nothing
    }

    private static SignalBatchData instance = new SignalBatchData();
  }

  @Override
  public boolean hasCurrent() {
    throw new UnsupportedOperationException("hasCurrent is not supported for SignalBatchData");
  }
}
