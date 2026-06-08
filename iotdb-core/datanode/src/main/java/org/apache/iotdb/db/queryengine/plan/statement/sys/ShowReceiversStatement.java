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

package org.apache.iotdb.db.queryengine.plan.statement.sys;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowStatement;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class ShowReceiversStatement extends ShowStatement {

  private static final List<SortItem> DEFAULT_SORT_ITEMS =
      ImmutableList.of(
          new SortItem(ColumnHeaderConstant.RECEIVER_NODE_TYPE, Ordering.ASC),
          new SortItem(ColumnHeaderConstant.RECEIVER_NODE_ID, Ordering.ASC),
          new SortItem(ColumnHeaderConstant.PROTOCOL, Ordering.ASC),
          new SortItem(ColumnHeaderConstant.SENDER_ADDRESS, Ordering.ASC),
          new SortItem(ColumnHeaderConstant.RECEIVER_USER_NAME, Ordering.ASC));

  public ShowReceiversStatement() {
    this.statementType = StatementType.SHOW_RECEIVERS;
  }

  @Override
  public boolean isQuery() {
    return true;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowReceivers(this, context);
  }

  public List<SortItem> getSortItemList() {
    return DEFAULT_SORT_ITEMS;
  }
}
