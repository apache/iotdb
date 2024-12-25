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

package org.apache.iotdb.db.queryengine.plan.relational.metadata;

import java.util.List;

/**
 * This class acts as a request for device schema validation and defines the necessary information
 * interfaces.
 *
 * <p>e.g. {database"db", "t1", [["hebei", "p_1", "t_1"], ["shandong", null, "t_1"]], ["attr_1",
 * "attr_2"], [["attr_value1", "attr_value2"], ["v_1", null]]}
 *
 * <ol>
 *   <li>database = "db"
 *   <li>tableName = "t1"
 *   <li>deviceIdList = [["hebei", "p_1", "t_1"], ["shandong", null, "t_1"]]
 *   <li>attributeColumnNameList = ["attr_1", "attr_2"]
 *   <li>attributeValueList = [["attr_value1", "attr_value2"], ["v_1", null]]
 * </ol>
 */
public interface ITableDeviceSchemaValidation {

  /**
   * @return database name
   */
  String getDatabase();

  /**
   * @return table name without database name as prefix
   */
  String getTableName();

  /**
   * @return ids, without db or table name, of all involved devices
   */
  List<Object[]> getDeviceIdList();

  /**
   * @return attribute column names
   */
  List<String> getAttributeColumnNameList();

  /**
   * @return attribute values, the order of which shall be consistent with that of the provided
   *     device ids and attribute column names.
   */
  List<Object[]> getAttributeValueList();
}
