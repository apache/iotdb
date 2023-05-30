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

package org.apache.iotdb.db.mpp.execution.operator.schema.source;

import org.apache.iotdb.db.metadata.query.info.ISchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import java.util.List;

public interface ISchemaSource<T extends ISchemaInfo> {

  /**
   * Get the SchemaReader for iterating target SchemaInfo from given SchemaRegion
   *
   * @return the SchemaReader for SchemaInfo T
   */
  ISchemaReader<T> getSchemaReader(ISchemaRegion schemaRegion);

  /** Get the column headers of the result TsBlock for SchemaInfo query */
  List<ColumnHeader> getInfoQueryColumnHeaders();

  /**
   * Transform the SchemaInfo T to fill the TsBlock
   *
   * @param schemaInfo the SchemaInfo need to be processed
   * @param tsBlockBuilder the target TsBlockBuilder using for generating TsBlock
   * @param database the belonged databased of given SchemaInfo
   */
  void transformToTsBlockColumns(T schemaInfo, TsBlockBuilder tsBlockBuilder, String database);

  boolean hasSchemaStatistic(ISchemaRegion schemaRegion);

  long getSchemaStatistic(ISchemaRegion schemaRegion);

  /**
   * Some tasks may be delayed, and will be processed before building and returning of
   * tsBlockBuilder. Those delayed tasks will be processed here. For example, in 'show timeseries'
   * statement, tasks of processing series of views will be delayed. These tasks will be processed
   * here, make counts of schema fetching come down.
   *
   * @param tsBlockBuilder complete delayed tasks on given tsBlock builder.
   */
  void processDelayedTask(TsBlockBuilder tsBlockBuilder, String database);
}
