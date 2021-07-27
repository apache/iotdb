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
package org.apache.iotdb.db.metadata;

public class MetadataOperationType {

  private MetadataOperationType() {
    // allowed to do nothing
  }

  public static final String CREATE_TIMESERIES = "0";
  public static final String DELETE_TIMESERIES = "1";
  public static final String SET_STORAGE_GROUP = "2";
  public static final String SET_TTL = "10";
  public static final String DELETE_STORAGE_GROUP = "11";
  public static final String CREATE_INDEX = "31";
  public static final String DROP_INDEX = "32";
  public static final String CHANGE_OFFSET = "12";
  public static final String CHANGE_ALIAS = "13";
  public static final String AUTO_CREATE_DEVICE = "4";
  public static final String CREATE_TEMPLATE = "5";
  public static final String SET_TEMPLATE = "6";
  public static final String SET_USING_TEMPLATE = "61";
}
