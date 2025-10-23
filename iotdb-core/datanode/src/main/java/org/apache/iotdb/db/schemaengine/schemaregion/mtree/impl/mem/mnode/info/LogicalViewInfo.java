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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpressionType;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

/**
 * This structure is used in ViewMNode. It stores all information except the name of this view. The
 * name of the view is stored by ViewMNode.
 */
public class LogicalViewInfo implements IMeasurementInfo {

  /** tag/attribute's start offset in tag file */
  private long offset = -1;

  /** whether this measurement is pre deleted and considered in black list */
  private boolean preDeleted = false;

  private LogicalViewSchema schema;

  public LogicalViewInfo(LogicalViewSchema schema) {
    this.schema = schema;
  }

  // region logical view interfaces
  public boolean isAliasSeries() {
    if (this.getExpression() != null) {
      if (this.getExpression().isSourceForAliasSeries()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return return the path of alias series if this view is alias series; else return null.
   */
  public PartialPath getAliasSeriesPath() {
    if (this.isAliasSeries()) {
      if (this.getExpression().getExpressionType() == ViewExpressionType.TIMESERIES) {
        String pathString = ((TimeSeriesViewOperand) this.getExpression()).getPathString();
        try {
          return new MeasurementPath(pathString);
        } catch (IllegalPathException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  public ViewExpression getExpression() {
    return this.schema.getExpression();
  }

  public void setExpression(ViewExpression expression) {
    this.schema.setExpression(expression);
  }

  // endregion

  // region IMeasurementInfo interfaces

  @Override
  public IMeasurementSchema getSchema() {
    return this.schema;
  }

  @Override
  public void setSchema(IMeasurementSchema schema) {
    if (schema.isLogicalView()) {
      this.schema = (LogicalViewSchema) schema;
    }
  }

  @Override
  public TSDataType getDataType() {
    return null;
  }

  @Override
  public String getAlias() {
    return null;
  }

  @Override
  public void setAlias(String alias) {
    // can not set alias for a logical view
    throw new UnsupportedOperationException("View doesn't support alias");
  }

  @Override
  public long getOffset() {
    // tag/attribute's start offset in tag file
    return offset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public boolean isPreDeleted() {
    return this.preDeleted;
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {
    this.preDeleted = preDeleted;
  }

  /**
   * The memory occupied by an MeasurementInfo based occupation
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>offset, 8B
   *   <li>boolean preDeleted, 1B
   *   <li>estimated schema size, 32B
   *   <li>viewExpression
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + 1 + 32 + ViewExpression.getSerializeSize(schema.getExpression());
  }

  @Override
  public void moveDataToNewMNode(IMeasurementMNode<?> newMNode) {
    if (newMNode.isLogicalView()) {
      newMNode.setSchema(this.schema);
      newMNode.setPreDeleted(preDeleted);
    }
    throw new SchemaExecutionException(
        new IllegalArgumentException(
            "Type of newMNode is not LogicalViewMNode! It's "
                + newMNode.getMNodeType().toString()));
  }
  // endregion
}
