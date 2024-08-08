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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

public class ConsensusWriteResponse extends ConsensusResponse {

  private final TSStatus status;

  public ConsensusWriteResponse(ConsensusException exception, TSStatus status) {
    super(exception);
    this.status = status;
  }

  public TSStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "ConsensusWriteResponse{" + "status=" + status + "} " + super.toString();
  }

  public boolean isSuccessful() {
    return status != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public String getErrorMessage() {
    if (status != null && status.message != null && status.message.length() > 0) {
      return status.message;
    }
    if (exception != null
        && exception.getMessage() != null
        && exception.getMessage().length() > 0) {
      return exception.getMessage();
    }
    return "unknown error message";
  }

  public static ConsensusWriteResponse.Builder newBuilder() {
    return new ConsensusWriteResponse.Builder();
  }

  public static class Builder {
    private TSStatus status;
    private ConsensusException exception;

    public ConsensusWriteResponse build() {
      return new ConsensusWriteResponse(exception, status);
    }

    public Builder setException(ConsensusException exception) {
      this.exception = exception;
      return this;
    }

    public Builder setStatus(TSStatus status) {
      this.status = status;
      return this;
    }
  }
}
