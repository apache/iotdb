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

package org.apache.iotdb.rpc.i18n;

public final class RpcMessages {

  // TElasticFramedTransport - FrameError
  public static final String FRAME_ERROR_HTTP_REQUEST =
      "Singular frame size (%d) detected, you may be sending HTTP GET/POST%s "
          + "requests to the Thrift-RPC port, please confirm that you are using the right port";
  public static final String FRAME_ERROR_TLS_REQUEST =
      "Singular frame size (%d) detected, you may be sending TLS ClientHello "
          + "requests%s to the Non-SSL Thrift-RPC port, please confirm that you are using "
          + "the right configuration";
  public static final String FRAME_ERROR_NEGATIVE_FRAME_SIZE =
      "Read a negative frame size (%d)%s!";
  public static final String FRAME_ERROR_FRAME_SIZE_EXCEEDED =
      "Frame size (%d) larger than protect max size (%d)%s!";
  public static final String FRAME_ERROR_STRING_LENGTH_EXCEEDED =
      "String length (%d) larger than protect max size (%d)%s!";

  // TElasticFramedTransport - SSL
  public static final String NON_SSL_TO_SSL_PORT =
      "You may be sending non-SSL requests"
          + "%s to the SSL-enabled Thrift-RPC port, please confirm that you are "
          + "using the right configuration";

  // ConfigurableTByteBuffer
  public static final String UNEXPECTED_END_OF_INPUT = "Unexpected end of input buffer";
  public static final String NOT_ENOUGH_ROOM_IN_OUTPUT = "Not enough room in output buffer";

  // BaseRpcTransportFactory
  public static final String COULD_NOT_LOAD_KEYSTORE =
      "Could not load keystore or truststore file";

  // AutoResizingBuffer
  public static final String AUTO_RESIZING_BUFFER_ALLOCATE_INTERRUPTED =
      "AutoResizingBuffer was interrupted while allocating %d bytes";
  public static final String AUTO_RESIZING_BUFFER_ALLOCATE_FAILED =
      "AutoResizingBuffer failed to allocate %d bytes after %d retries";

  // IoTDBRpcDataSet / IoTDBJDBCDataSet
  public static final String CLOSE_OPERATION_SERVER_ERROR =
      "Error occurs for close operation in server side because ";
  public static final String CLOSE_OPERATION_CONNECTION_ERROR =
      "Error occurs when connecting to server for close operation ";
  public static final String DATASET_ALREADY_CLOSED = "This DataSet is already closed";
  public static final String COLUMN_INDEX_SHOULD_START_FROM_1 =
      "column index should start from 1";
  public static final String COLUMN_INDEX_OUT_OF_RANGE =
      "column index %d out of range %d";
  public static final String UNKNOWN_COLUMN_NAME = "Unknown column name: ";
  public static final String NO_RECORD_REMAINS = "No record remains";
  public static final String CANNOT_CLOSE_DATASET =
      "Cannot close dataset, because of network connection: {} ";

  // RpcUtils
  public static final String UNKNOWN_TIME_PRECISION = "Unknown time precision: ";
  public static final String UNKNOWN_TIME_FACTOR = "Unknown time factor: ";

  // PreparedParameterSerde
  public static final String FAILED_TO_SERIALIZE_PARAMETERS =
      "Failed to serialize parameters";
  public static final String INVALID_PARAMETER_COUNT = "Invalid parameter count: ";
  public static final String UNSUPPORTED_TYPE = "Unsupported type: ";

  // SynchronizedHandler
  public static final String ERROR_IN_CALLING_METHOD = "Error in calling method ";

  // Shared fragments
  public static final String EMPTY_MESSAGE = "";
  public static final String REMOTE_ADDRESS_PREFIX = " from ";

  private RpcMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_SIZE_COLUMNINDEX2TSBLOCKCOLUMNINDEXLIST_ARG_DOESN_T_EQUAL_SIZE_COLUMNNAMELIST_ARG_F1209A2B = "Size of columnIndex2TsBlockColumnIndexList %s doesn't equal to size of columnNameList %s.";
  public static final String EXCEPTION_CANNOT_FETCH_RESULT_SERVER_BECAUSE_NETWORK_CONNECTION_ARG_24BE1326 = "Cannot fetch result from server, because of network connection: {} ";
  public static final String EXCEPTION_ARG_ARG_046AFB8B = "%d: %s";
  public static final String EXCEPTION_LATER_REQUEST_SAME_GROUP_WILL_REDIRECTED_0A61CB0B = "later request in same group will be redirected to ";
  public static final String EXCEPTION_NO_SUPPORTED_KEYSTORE_OR_TRUSTSTORE_TYPE_IS_AVAILABLE_B6EA1528 =
      "No supported keystore or truststore type is available";
  public static final String EXCEPTION_FAILED_TO_LOAD_KEYSTORE_OR_TRUSTSTORE_FILE_F3306313 =
      "Failed to load keystore or truststore file";
  public static final String EXCEPTION_KEYSTORE_OR_TRUSTSTORE_FILE_NOT_FOUND_5A6845B2 =
      "keystore or truststore file not found: ";

}
