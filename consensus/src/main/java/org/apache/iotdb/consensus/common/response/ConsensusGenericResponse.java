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

package org.apache.iotdb.consensus.common.response;

import org.apache.iotdb.consensus.exception.ConsensusException;

public class ConsensusGenericResponse extends ConsensusResponse {

  private final boolean success;

  public ConsensusGenericResponse(ConsensusException exception, boolean success) {
    super(exception);
    this.success = success;
  }

  public boolean isSuccess() {
    return success;
  }

  @Override
  public String toString() {
    return "ConsensusGenericResponse{" + "success=" + success + "} " + super.toString();
  }

  public static ConsensusGenericResponse.Builder newBuilder() {
    return new ConsensusGenericResponse.Builder();
  }

  public static class Builder {
    private boolean success;
    private ConsensusException exception;

    public ConsensusGenericResponse build() {
      return new ConsensusGenericResponse(exception, success);
    }

    public ConsensusGenericResponse.Builder setException(ConsensusException exception) {
      this.exception = exception;
      return this;
    }

    public ConsensusGenericResponse.Builder setSuccess(boolean success) {
      this.success = success;
      return this;
    }
  }
}
