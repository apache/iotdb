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

package org.apache.iotdb.confignode.client;

public enum CnToCnNodeRequestType {
  ADD_CONSENSUS_GROUP,
  NOTIFY_REGISTER_SUCCESS,
  REGISTER_CONFIG_NODE,
  RESTART_CONFIG_NODE,
  REMOVE_CONFIG_NODE,
  DELETE_CONFIG_NODE_PEER,
  REPORT_CONFIG_NODE_SHUTDOWN,
  STOP_AND_CLEAR_CONFIG_NODE,
  SET_CONFIGURATION,
  SHOW_CONFIGURATION,
  SHOW_APPLIED_CONFIGURATIONS,
  SUBMIT_TEST_CONNECTION_TASK,
  TEST_CONNECTION,
}
