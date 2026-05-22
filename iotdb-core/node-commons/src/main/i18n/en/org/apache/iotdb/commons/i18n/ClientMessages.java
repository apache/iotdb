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

package org.apache.iotdb.commons.i18n;

public final class ClientMessages {

  // ClientManager
  public static final String RETURN_CLIENT_TO_POOL_FAILED =
      "Return client {} for node {} to pool failed.";
  public static final String RETURN_CLIENT_NULL_NODE =
      "Return client {} to pool failed because the node is null. "
          + "This may cause resource leak, please check your code.";
  public static final String CLEAR_CLIENT_POOL_FAILED =
      "Clear all client in pool for node {} failed.";

  // ThriftClient
  public static final String EXCEPTION_LEVEL_DETAIL =
      "level-{} Exception class {}, message {}";
  public static final String ROOT_CAUSE_DETAIL =
      "root cause message {}, LocalizedMessage {}, ";
  public static final String BROKEN_PIPE_CLEAR_CONNECTIONS =
      "Broken pipe error happened in sending RPC,"
          + " we need to clear all previous cached connection, error msg is {}";

  // Async/Sync clients - invalidation
  public static final String CLIENT_INVALIDATED = "This client has been invalidated";
  public static final String CLIENT_INVALIDATED_WITH_ID =
      "This client %d has been invalidated";

  // Async clients - readiness check
  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT =
      "Unexpected exception occurs in {} : {}";
  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT_WITH_MSG =
      "Unexpected exception occurs in {}, error msg is {}";

  // Async clients - timeout management
  public static final String TIMEOUT_ALREADY_SET =
      "This client's timeout has been set to {}."
          + " If you need to set it to {}, please call the recoverTimeout() first.";
  public static final String TIMEOUT_NOT_MODIFIED =
      "This client's timeout has not been modified, cannot reset";

  // AsyncPipeDataTransferServiceClient
  public static final String FAILED_TO_SET_TIMEOUT_DYNAMICALLY =
      "Failed to set timeout dynamically, set it statically";
  public static final String HANDSHAKE_FINISHED = "Handshake finished for client {}";
  public static final String MANUALLY_CLOSING_TRANSPORT =
      "Manually closing transport to prevent resource leakage.";
  public static final String METHOD_STATE_RESET =
      "Method state has been reset due to manager not running.";

  // AsyncThriftClientFactory
  public static final String CANNOT_CREATE_ASYNC_FACTORY =
      "Cannot create Async thrift client factory %s";

  // SyncThriftClientWithErrorHandler
  public static final String ERROR_IN_CALLING_METHOD =
      "Error in calling method %s, because: %s";

  // BorrowNullClientManagerException
  public static final String CANNOT_BORROW_CLIENT_NULL_NODE =
      "Can not borrow client for node null";

  // AsyncRequestManager
  public static final String ASYNC_REQUEST_TIMEOUT =
      "Timeout during {}. Retry: {}/{}";
  public static final String ASYNC_REQUEST_INTERRUPTED =
      "Interrupted during {}. Retry: {}/{}";
  public static final String ASYNC_REQUEST_FAILED_AFTER_RETRIES =
      "Failed to {} after {} retries, requestIndices: {}";
  public static final String UNSUPPORTED_REQUEST_TYPE =
      "unsupported request type %s,"
          + " please set it in AsyncRequestManager::initActionMapBuilder()";
  public static final String ASYNC_REQUEST_FAILED_ON_NODE =
      "{} failed on Node {}, because {}, retrying {}...";

  private ClientMessages() {}
}
