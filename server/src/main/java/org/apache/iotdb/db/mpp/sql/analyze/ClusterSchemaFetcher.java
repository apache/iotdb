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
package org.apache.iotdb.db.mpp.sql.analyze;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaInternalNode;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.execution.Coordinator;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SchemaFetchStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.List;

public class ClusterSchemaFetcher implements ISchemaFetcher {

  private final Coordinator coordinator = Coordinator.getInstance();

  @Override
  public SchemaTree fetchSchema(PathPatternTree patternTree) {
    SchemaFetchStatement schemaFetchStatement = new SchemaFetchStatement(patternTree);
    QueryId queryId =
        new QueryId(String.valueOf(SessionManager.getInstance().requestQueryId(false)));
    coordinator.execute(schemaFetchStatement, queryId, QueryType.READ, null, "");
    TsBlock tsBlock = coordinator.getResultSet(queryId);
    SchemaTree result = new SchemaTree(new SchemaInternalNode("root"));
    while (tsBlock.hasNext()) {
      Binary binary = tsBlock.getColumn(0).getBinary(0);
      SchemaTree schemaTree = SchemaTree.deserialize(ByteBuffer.wrap(binary.getValues()));
    }

    return result;
  }

  @Override
  public SchemaTree fetchSchemaWithAutoCreate(
      PartialPath devicePath, String[] measurements, TSDataType[] tsDataTypes) {
    return null;
  }

  @Override
  public SchemaTree fetchSchemaListWithAutoCreate(
      List<PartialPath> devicePath, List<String[]> measurements, List<TSDataType[]> tsDataTypes) {
    return null;
  }
}
