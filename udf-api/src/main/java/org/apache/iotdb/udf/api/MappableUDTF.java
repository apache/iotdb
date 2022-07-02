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

package org.apache.iotdb.udf.api;

import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.strategy.MappableRowByRowAccessStrategy;

import java.io.IOException;

public interface MappableUDTF extends UDTF {

  /**
   * When the user specifies {@link MappableRowByRowAccessStrategy} to access the original data in
   * {@link UDTFConfigurations}, this method will be called to process the transformation. In a
   * single UDF query, this method may be called multiple times.
   *
   * @param row original input data row (aligned by time)
   * @throws Exception the user can throw errors if necessary
   * @see MappableRowByRowAccessStrategy
   */
  Object transform(Row row) throws IOException;
}
