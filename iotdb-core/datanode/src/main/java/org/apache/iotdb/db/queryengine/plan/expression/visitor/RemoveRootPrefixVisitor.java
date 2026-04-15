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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;

import static org.apache.iotdb.commons.schema.SchemaConstant.ROOT;

public class RemoveRootPrefixVisitor extends ReconstructVisitor<Void> {

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    if (!rawPath.startsWith(ROOT)) {
      return timeSeriesOperand;
    }

    String[] rawPartialNodes = rawPath.getNodes();

    int newPartialNodesLength = rawPartialNodes.length - 1;
    String[] newPartialNodes = new String[newPartialNodesLength];
    System.arraycopy(rawPartialNodes, 1, newPartialNodes, 0, newPartialNodesLength);

    return new TimeSeriesOperand(new PartialPath(newPartialNodes), timeSeriesOperand.getType());
  }
}
