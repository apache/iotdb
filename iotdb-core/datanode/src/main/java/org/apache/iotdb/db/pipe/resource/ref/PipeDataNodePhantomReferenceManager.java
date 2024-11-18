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

package org.apache.iotdb.db.pipe.resource.ref;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.ref.PipePhantomReferenceManager;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;

public class PipeDataNodePhantomReferenceManager extends PipePhantomReferenceManager {

  public PipeDataNodePhantomReferenceManager() {
    super();

    PipeDataNodeAgent.runtime()
        .registerPhantomReferenceCleanJob(
            "PipePhantomReferenceManager#gcHook()",
            // NOTE: lambda CAN NOT be replaced with method reference
            () -> super.gcHook(),
            PipeConfig.getInstance().getPipeEventReferenceEliminateIntervalSeconds());
  }
}
