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

package org.apache.iotdb.udf.api.customizer.config;

import org.apache.iotdb.udf.api.relational.AggregateFunction;
import org.apache.iotdb.udf.api.type.Type;

public class AggregateFunctionConfig extends UDFConfigurations {

  private boolean isRemovable = false;

  /**
   * Set the output data type of the scalar function.
   *
   * @param outputDataType the output data type of the scalar function
   * @return this
   */
  public AggregateFunctionConfig setOutputDataType(Type outputDataType) {
    this.outputDataType = outputDataType;
    return this;
  }

  /**
   * If aggregate function is removable, {@linkplain AggregateFunction#remove} should be
   * implemented.
   *
   * @param removable whether the aggregate function is removable
   */
  public void setRemovable(boolean removable) {
    isRemovable = removable;
  }

  public boolean isRemovable() {
    return isRemovable;
  }
}
