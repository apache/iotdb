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

package org.apache.iotdb.rpc.subscription.config;

public class ConsumerConstant {

  /////////////////////////////// common ///////////////////////////////

  public static final String HOST_KEY = "host";
  public static final String PORT_KEY = "port";
  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";

  public static final String CONSUMER_ID_KEY = "consumer-id";
  public static final String CONSUMER_GROUP_ID_KEY = "group-id";

  /////////////////////////////// pull consumer ///////////////////////////////

  public static final String AUTO_COMMIT_KEY = "auto-commit";
  public static final boolean AUTO_COMMIT_DEFAULT_VALUE = true;

  public static final String AUTO_COMMIT_INTERVAL_KEY = "auto-commit-interval"; // unit: ms
  public static final int AUTO_COMMIT_INTERVAL_DEFAULT_VALUE = 5000;
  public static final int AUTO_COMMIT_INTERVAL_MIN_VALUE = 500;

  /////////////////////////////// push consumer ///////////////////////////////

  public static final String ACK_STRATEGY_KEY = "ack-strategy";
  public static final String ACK_STRATEGY_BEFORE_CONSUME = "before-consume";
  public static final String ACK_STRATEGY_AFTER_CONSUME = "after-consume";
  public static final String ACK_STRATEGY_DEFAULT_VALUE = ACK_STRATEGY_AFTER_CONSUME;

  // TODO: configure this parameter
  public static final int PUSH_CONSUMER_AUTO_POLL_INTERVAL = 1000;
  public static final int PUSH_CONSUMER_AUTO_POLL_TIME_OUT = 2000;

  private ConsumerConstant() {
    throw new IllegalStateException("Utility class");
  }
}
