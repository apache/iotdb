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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.consensus.raft.core.listener.ListenerEvent;

/**
 * L3 runtime invariants: a violated check is reported as an {@code assert} event (metrics count it)
 * and, under {@code strictAsserts}, thrown so that the group stops instead of continuing in a
 * corrupt state.
 */
public final class Invariants {

  private Invariants() {}

  public static void check(NodeEnv env, boolean ok, String invariant, String message) {
    if (ok) {
      return;
    }
    env.listener()
        .onEvent(
            ListenerEvent.of("assert", env.self())
                .with("inv", invariant)
                .with("msg", message)
                .build());
    if (env.policy().strictAsserts) {
      throw new IllegalStateException(invariant + " violated at N" + env.self() + ": " + message);
    }
  }
}
