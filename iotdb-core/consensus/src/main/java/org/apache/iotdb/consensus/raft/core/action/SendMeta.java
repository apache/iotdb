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

package org.apache.iotdb.consensus.raft.core.action;

/** Per-target metadata attached to a send (TRaft: v.rho, in-flight size, window; INV-P4). */
public final class SendMeta {

  public final long vRho;
  public final int flight;
  public final int window;
  public final boolean speculative;

  public SendMeta(long vRho, int flight, int window, boolean speculative) {
    this.vRho = vRho;
    this.flight = flight;
    this.window = window;
    this.speculative = speculative;
  }

  @Override
  public String toString() {
    return "{v_rho="
        + vRho
        + ",flight="
        + flight
        + ",w="
        + window
        + (speculative ? ",spec" : "")
        + "}";
  }
}
