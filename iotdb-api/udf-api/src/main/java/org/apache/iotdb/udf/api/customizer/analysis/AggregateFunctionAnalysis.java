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

package org.apache.iotdb.udf.api.customizer.analysis;

import org.apache.iotdb.udf.api.type.Type;

public class AggregateFunctionAnalysis implements FunctionAnalysis {
  private final Type outputDataType;
  private final boolean removable;

  private AggregateFunctionAnalysis(Type outputDataType, boolean removable) {
    this.outputDataType = outputDataType;
    this.removable = removable;
  }

  public Type getOutputDataType() {
    return outputDataType;
  }

  public boolean isRemovable() {
    return removable;
  }

  public static class Builder {
    private Type outputDataType;
    private boolean removable = false;

    public Builder outputDataType(Type outputDataType) {
      this.outputDataType = outputDataType;
      return this;
    }

    public Builder removable(boolean removable) {
      this.removable = removable;
      return this;
    }

    public AggregateFunctionAnalysis build() throws IllegalArgumentException {
      if (outputDataType == null) {
        throw new IllegalArgumentException("AggregateFunctionAnalysis outputDataType is not set.");
      }
      return new AggregateFunctionAnalysis(outputDataType, removable);
    }
  }
}
