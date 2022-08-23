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
package org.apache.iotdb.db.mpp.execution.operator.process.fill.identity;

import org.apache.iotdb.db.mpp.execution.operator.process.fill.ILinearFill;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class IdentityLinearFill implements ILinearFill {

  @Override
  public Column fill(TimeColumn timeColumn, Column valueColumn, long currentRowIndex) {
    return valueColumn;
  }

  @Override
  public boolean needPrepareForNext(long rowIndex, Column valueColumn) {
    return false;
  }

  @Override
  public boolean prepareForNext(
      long startRowIndex, long endRowIndex, TimeColumn nextTimeColumn, Column nextValueColumn) {
    throw new IllegalArgumentException(
        "We won't call prepareForNext in IdentityLinearFill, because needPrepareForNext() method will always return false.");
  }
}
