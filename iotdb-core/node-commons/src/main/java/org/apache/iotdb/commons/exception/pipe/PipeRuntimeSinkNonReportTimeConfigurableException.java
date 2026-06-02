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

package org.apache.iotdb.commons.exception.pipe;

import java.util.Objects;

public class PipeRuntimeSinkNonReportTimeConfigurableException
    extends PipeRuntimeSinkCriticalException {

  private final long interval;

  public PipeRuntimeSinkNonReportTimeConfigurableException(
      final String message, final long interval) {
    super(message);
    this.interval = interval;
  }

  public long getInterval() {
    return interval;
  }

  // We do not record the timestamp here for logger reduction detection
  @Override
  public String toString() {
    return "PipeRuntimeSinkNonReportTimeConfigurableException{"
        + "message='"
        + "', interval='"
        + interval
        + "'}";
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeRuntimeSinkNonReportTimeConfigurableException that =
        (PipeRuntimeSinkNonReportTimeConfigurableException) o;
    return super.equals(that) && interval == that.interval;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), interval);
  }
}
