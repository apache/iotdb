/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowNodesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class NodeSchemaSource implements ISchemaSource<INodeSchemaInfo> {

  private final PartialPath pathPattern;
  private final PathPatternTree scope;
  private final int level;

  NodeSchemaSource(PartialPath pathPattern, int level, PathPatternTree scope) {
    this.pathPattern = pathPattern;
    this.level = level;
    this.scope = scope;
  }

  @Override
  public ISchemaReader<INodeSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    IShowNodesPlan showNodesPlan;
    if (-1 == level) {
      showNodesPlan =
          SchemaRegionReadPlanFactory.getShowNodesPlan(
              pathPattern.concatNode(ONE_LEVEL_PATH_WILDCARD), scope);
    } else {
      showNodesPlan =
          SchemaRegionReadPlanFactory.getShowNodesPlan(pathPattern, level, false, scope);
    }
    try {
      return schemaRegion.getNodeReader(showNodesPlan);
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return ColumnHeaderConstant.showChildPathsColumnHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      INodeSchemaInfo nodeSchemaInfo, TsBlockBuilder tsBlockBuilder, String database) {
    tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
    tsBlockBuilder
        .getColumnBuilder(0)
        .writeBinary(new Binary(nodeSchemaInfo.getFullPath(), TSFileConfig.STRING_CHARSET));
    tsBlockBuilder
        .getColumnBuilder(1)
        .writeBinary(
            new Binary(
                String.valueOf(nodeSchemaInfo.getNodeType().getNodeType()),
                TSFileConfig.STRING_CHARSET));
    tsBlockBuilder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return false;
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return 0;
  }
}
