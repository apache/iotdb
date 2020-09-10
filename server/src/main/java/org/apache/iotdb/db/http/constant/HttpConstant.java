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
package org.apache.iotdb.db.http.constant;

public class HttpConstant {

  //router
  public static final String ROUTING_STORAGE_GROUPS = "/storageGroups";
  public static final String ROUTING_TIME_SERIES = "/timeSeries";
  public static final String ROUTING_USER_LOGIN = "/user/login";
  public static final String ROUTING_USER_LOGOUT = "/user/logout";
  public static final String ROUTING_QUERY = "/query";
  public static final String ROUTING_INSERT = "/insert";
  public static final String ROUTING_STORAGE_GROUPS_DELETE = "/storageGroups/delete";
  public static final String ROUTING_TIME_SERIES_DELETE = "/timeSeries/delete";
  public static final String ROUTING_GET_TIME_SERIES = "/getTimeSeries";

  public static final char QUESTION_MARK = '?';
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String SUCCESSFUL_OPERATION = "successful operation";
  public static final String RESULT = "result";
  public static final String STORAGE_GROUP = "storage group";
  public static final String TTL = "TTL";
  public static final String TIME_SERIES = "timeSeries";
  public static final String ALIAS = "alias";
  public static final String DATATYPE = "dataType";
  public static final String ENCODING = "encoding";
  public static final String COMPRESSION = "compression";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  public static final String PROPERTIES = "properties";
  public static final String TAGS = "tags";
  public static final String ATTRIBUTES = "attributes";
  public static final String NULL = "null";

  public static final String DEVICE_ID  = "deviceId";
  public static final String MEASUREMENTS = "measurements";
  public static final String TIMESTAMPS = "timestamps";
  public static final String VALUES = "values";
  public static final String RANGE = "range";
  public static final String FROM = "from";
  public static final String SELECT = "select";
  public static final String START = "start";
  public static final String END = "end";
  public static final String ERROR = "error";
}
