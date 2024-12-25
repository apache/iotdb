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

package org.apache.iotdb.confignode.manager.pipe.agent.task;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskExecutor;
import org.apache.iotdb.commons.utils.TestOnly;

public class PipeConfigNodeSubtaskExecutor extends PipeSubtaskExecutor {

  private static final int THREAD_NUM = 1;

  private PipeConfigNodeSubtaskExecutor() {
    super(THREAD_NUM, ThreadName.PIPE_CONFIGNODE_EXECUTOR_POOL, true);
  }

  /**
   * @param ignored Used to distinguish this constructor from the default constructor.
   */
  @TestOnly
  public PipeConfigNodeSubtaskExecutor(final Object ignored) {
    super(THREAD_NUM, ThreadName.PIPE_CONFIGNODE_EXECUTOR_POOL, true);
  }

  private static class PipeSchemaSubtaskExecutorHolder {
    private static PipeConfigNodeSubtaskExecutor instance = null;
  }

  public static synchronized PipeConfigNodeSubtaskExecutor getInstance() {
    if (PipeSchemaSubtaskExecutorHolder.instance == null) {
      PipeSchemaSubtaskExecutorHolder.instance = new PipeConfigNodeSubtaskExecutor();
    }
    return PipeSchemaSubtaskExecutorHolder.instance;
  }
}
