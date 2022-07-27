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
package org.apache.iotdb.db.sync;

import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.qp.physical.sys.CreatePipeSinkPlan;
import org.apache.iotdb.db.sync.sender.pipe.PipeSink;

public class SyncUtils {
  public static PipeSink parseCreatePipeSinkPlan(CreatePipeSinkPlan plan) throws PipeSinkException {
    PipeSink pipeSink;
    try {
      pipeSink =
          PipeSink.PipeSinkFactory.createPipeSink(plan.getPipeSinkType(), plan.getPipeSinkName());
    } catch (UnsupportedOperationException e) {
      throw new PipeSinkException(e.getMessage());
    }

    pipeSink.setAttribute(plan.getPipeSinkAttributes());
    return pipeSink;
  }
}
