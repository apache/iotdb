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

package org.apache.iotdb.db.queryengine.transformation.dag.column.leaf;

import org.apache.tsfile.read.common.block.column.NullColumn;

import static org.apache.tsfile.read.common.type.UnknownType.UNKNOWN;

public class NullColumnTransformer extends LeafColumnTransformer {

  public NullColumnTransformer() {
    super(UNKNOWN);
  }

  @Override
  protected void evaluate() {
    initializeColumnCache(new NullColumn(input.getPositionCount()));
  }

  @Override
  public void evaluateWithSelection(boolean[] selection) {
    initializeColumnCache(new NullColumn(input.getPositionCount()));
  }
}
