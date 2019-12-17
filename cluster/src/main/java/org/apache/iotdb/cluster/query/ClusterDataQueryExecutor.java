/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import java.io.IOException;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.EngineExecutor;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public class ClusterDataQueryExecutor extends EngineExecutor {

  private QueryExpression queryExpression;
  private MetaGroupMember metaGroupMember;


  ClusterDataQueryExecutor(QueryExpression queryExpression, MetaGroupMember metaGroupMember) {
    super(queryExpression);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected IPointReader getSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
      QueryContext context, boolean pushdownUnseq) throws IOException, StorageEngineException {
    return metaGroupMember.getSeriesReader(path, timeFilter, context, pushdownUnseq, false);
  }

  @Override
  protected IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context)
      throws IOException, StorageEngineException {
    return metaGroupMember.getReaderByTimestamp(path, context);
  }

  @Override
  protected TSDataType getDataType(String path) throws MetadataException {
    return metaGroupMember.getSeriesType(path);
  }

  @Override
  protected EngineTimeGenerator getTimeGenerator(IExpression queryExpression,
      QueryContext context) throws StorageEngineException {
    return new ClusterTimeGenerator(queryExpression, context, metaGroupMember);
  }
}
