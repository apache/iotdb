/**
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
package org.apache.iotdb.jdbc;

public class Constant {

  private Constant(){}

  public static final String GLOBAL_DB_NAME = "IoTDB";

  public static final String GLOBAL_DB_VERSION = "0.8.0-SNAPSHOT";

  public static final String GLOBAL_COLUMN_REQ = "COLUMN";

  public static final String GLOBAL_DELTA_OBJECT_REQ = "DELTA_OBEJECT";

  public static final String GLOBAL_SHOW_TIMESERIES_REQ = "SHOW_TIMESERIES";

  public static final String GLOBAL_COUNT_TIMESERIES_REQ = "COUNT_TIMESERIES";

  public static final String GLOBAL_COUNT_NODE_TIMESERIES_REQ = "COUNT_NODE_TIMESERIES";

  public static final String GLOBAL_COUNT_NODES_REQ = "COUNT_NODES";

  public static final String GLOBAL_SHOW_STORAGE_GROUP_REQ = "SHOW_STORAGE_GROUP";

  public static final String GLOBAL_COLUMNS_REQ = "ALL_COLUMNS";

  // catalog parameters used for DatabaseMetaData.getColumns()
  public static final String CATALOG_COLUMN = "col";
  public static final String CATALOG_TIMESERIES = "ts";
  public static final String CATALOG_STORAGE_GROUP = "sg";
  public static final String CATALOG_DEVICE = "delta";
  public static final String COUNT_TIMESERIES = "cntts";
  public static final String COUNT_NODE_TIMESERIES = "cnttsbg";
  public static final String COUNT_NODES = "cntnode";
}
