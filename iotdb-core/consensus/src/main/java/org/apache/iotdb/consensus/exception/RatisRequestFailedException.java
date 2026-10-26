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

package org.apache.iotdb.consensus.exception;

import org.apache.iotdb.consensus.i18n.ConsensusMessages;

import java.util.Optional;

public class RatisRequestFailedException extends ConsensusException {

  public RatisRequestFailedException(Exception cause) {
    super(
        ConsensusMessages.EXCEPTION_RATIS_REQUEST_FAILED_52AF217F
            + Optional.ofNullable(cause)
                .map(Exception::getMessage)
                .orElse(ConsensusMessages.EXCEPTION_UNKNOWN_88183B94),
        cause);
  }

  public RatisRequestFailedException(String message, Exception cause) {
    super(
        ConsensusMessages.EXCEPTION_RATIS_REQUEST_FAILED_58107CDE
            + message
            + ConsensusMessages.EXCEPTION_DOT_F779BA66
            + Optional.ofNullable(cause)
                .map(Exception::getMessage)
                .orElse(ConsensusMessages.EXCEPTION_UNKNOWN_88183B94),
        cause);
  }
}
