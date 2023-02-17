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

package org.apache.iotdb.db.mpp.transformation.dag.column.leaf;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.type.Type;

/**
 * this is a special transformer which outputs data just as input without any modification.
 *
 * <p>i.e. it's just the function f(x) = x.
 */
public class IdentityColumnTransformer extends LeafColumnTransformer {
  // the index of value column
  private final int inputIndex;

  public IdentityColumnTransformer(Type returnType, int inputIndex) {
    super(returnType);
    this.inputIndex = inputIndex;
  }

  @Override
  public void initFromTsBlock(TsBlock input) {
    initializeColumnCache(input.getColumn(inputIndex));
  }
}
