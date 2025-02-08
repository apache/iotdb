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

package org.apache.iotdb.commons.pipe.metric;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

public abstract class PipeRemainingOperator {

  protected static final long REMAINING_MAX_SECONDS = 365 * 24 * 60 * 60L; // 1 year

  protected String pipeName;
  protected long creationTime = 0;

  private long lastEmptyTimeStamp = System.currentTimeMillis();
  private long lastNonEmptyTimeStamp = System.currentTimeMillis();
  protected boolean isStopped = true;

  protected PipeRemainingOperator(final String pipeName, final long creationTime) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
  }

  //////////////////////////// Tags ////////////////////////////

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  //////////////////////////// Switch ////////////////////////////

  protected void notifyNonEmpty() {
    final long pipeRemainingTimeCommitAutoSwitchSeconds =
        PipeConfig.getInstance().getPipeRemainingTimeCommitAutoSwitchSeconds();

    lastNonEmptyTimeStamp = System.currentTimeMillis();
    if (lastNonEmptyTimeStamp - lastEmptyTimeStamp
        >= pipeRemainingTimeCommitAutoSwitchSeconds * 1000) {
      thawRate(false);
    }
  }

  protected void notifyEmpty() {
    final long pipeRemainingTimeCommitAutoSwitchSeconds =
        PipeConfig.getInstance().getPipeRemainingTimeCommitAutoSwitchSeconds();

    lastEmptyTimeStamp = System.currentTimeMillis();
    if (lastEmptyTimeStamp - lastNonEmptyTimeStamp
        >= pipeRemainingTimeCommitAutoSwitchSeconds * 1000) {
      freezeRate(false);
    }
  }

  public synchronized void thawRate(final boolean isStartPipe) {
    if (isStartPipe) {
      isStopped = false;
    }
  }

  public synchronized void freezeRate(final boolean isStopPipe) {
    if (isStopPipe) {
      isStopped = true;
    }
  }
}
