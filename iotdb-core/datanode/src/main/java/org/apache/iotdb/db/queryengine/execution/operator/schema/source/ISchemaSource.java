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

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ISchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public interface ISchemaSource<T extends ISchemaInfo> {

  /**
   * Get the {@link ISchemaReader} for iterating target SchemaInfo from given {@link SchemaRegion}.
   *
   * @return the {@link ISchemaReader} for SchemaInfo T
   */
  ISchemaReader<T> getSchemaReader(final ISchemaRegion schemaRegion);

  /** Get the column headers of the result {@link TsBlock} for SchemaInfo query. */
  List<ColumnHeader> getInfoQueryColumnHeaders();

  /**
   * Transform the SchemaInfo T to fill the {@link TsBlock}.
   *
   * @param schemaInfo the SchemaInfo need to be processed
   * @param tsBlockBuilder the target {@link TsBlockBuilder} using for generating TsBlock
   * @param database the belonged databased of given SchemaInfo
   */
  void transformToTsBlockColumns(
      final T schemaInfo, final TsBlockBuilder tsBlockBuilder, final String database);

  boolean hasSchemaStatistic(final ISchemaRegion schemaRegion);

  long getSchemaStatistic(final ISchemaRegion schemaRegion);
}
