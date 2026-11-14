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

package org.apache.iotdb.consensus.raft.core.apply;

import org.apache.iotdb.consensus.raft.core.NodeEnv;
import org.apache.iotdb.consensus.raft.core.log.LogView;

/** What an {@link ApplyPolicy} may see and do on its node (provided by the kernel). */
public interface ApplyContext extends NodeEnv {

  LogView log();

  /**
   * Ask the applier to apply {@code [fromInclusive, toInclusive]} in index order; every index is
   * reported back through {@link ApplyPolicy#onApplied}.
   */
  void emitApply(long fromInclusive, long toInclusive);
}
