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
package org.apache.iotdb.mpp.execution.task;

/** the status enum of {@link FragmentInstanceTask} */
public enum FragmentInstanceTaskStatus {
  /* Ready to be executed */
  READY,

  /* Being executed */
  RUNNING,

  /* Waiting upstream input or output consumed by downstream FragmentInstances */
  BLOCKED,

  /* Interrupted caused by timeout or coordinator's cancellation */
  ABORTED,

  /* Finished by met the EOF of upstream inputs */
  FINISHED,
}
