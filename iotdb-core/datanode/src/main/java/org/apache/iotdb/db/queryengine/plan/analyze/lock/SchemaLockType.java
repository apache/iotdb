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

package org.apache.iotdb.db.queryengine.plan.analyze.lock;

/**
 * This enum class is used for maintaining types of locks used on DataNode for distributed
 * concurrency safety, especially for scenarios with DataNode and ConfigNode interactions.
 */
public enum SchemaLockType {

  /**
   * This lock is used for guarantee no timeseries under template set path.
   *
   * <ol>
   *   <li>Take read lock before creating timeseries.
   *   <li>Release read lock after finishing creating timeseries.
   *   <li>Take write lock before pre-set template.
   *   <li>Release write lock after finishing pre-set template.
   * </ol>
   */
  TIMESERIES_VS_TEMPLATE,

  /**
   * This lock is used for guarantee no data without schema after timeseries deletion.
   *
   * <ol>
   *   <li>Take read lock before validating schema during inserting data or loading TsFile.
   *   <li>Release read lock after finishing inserting data or loading TsFile.
   *   <li>Take write lock before invalidating schema cache during timeseries deletion.
   *   <li>Release write lock after finishing invalidating schema cache.
   * </ol>
   */
  VALIDATE_VS_DELETION,

  /**
   * This lock is used for guarantee no timeseries (tree model) under path representing table (table
   * model).
   *
   * <ol>
   *   <li>Take read lock before creating timeseries (tree model).
   *   <li>Release read lock after finishing creating timeseries (tree model).
   *   <li>Take write lock before pre-create table (table model).
   *   <li>Release write lock after finishing pre-create table (table model).
   * </ol>
   */
  TIMESERIES_VS_TABLE,
}
