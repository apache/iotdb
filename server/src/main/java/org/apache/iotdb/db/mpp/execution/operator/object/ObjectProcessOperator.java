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

package org.apache.iotdb.db.mpp.execution.operator.object;

import org.apache.iotdb.db.mpp.execution.object.ObjectEntry;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ObjectProcessOperator<T extends ObjectEntry> extends ObjectQueryOperator<T>
    implements ProcessOperator {

  public ObjectProcessOperator(OperatorContext operatorContext, String queryId) {
    super(operatorContext, queryId);
  }

  protected List<T> getNextObjectBatchFromChild(Operator childOperator) {
    TsBlock tsBlock = childOperator.next();
    if (tsBlock == null) {
      return null;
    }
    if (tsBlock.isEmpty()) {
      return Collections.emptyList();
    }

    List<T> objectEntryList = new ArrayList<>(tsBlock.getPositionCount());
    for (int i = 0; i < tsBlock.getPositionCount(); i++) {
      objectEntryList.add(objectPool.get(queryId, tsBlock.getColumn(0).getInt(i)));
    }
    return objectEntryList;
  }
}
