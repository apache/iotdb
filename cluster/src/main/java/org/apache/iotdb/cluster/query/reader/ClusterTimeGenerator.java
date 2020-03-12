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

package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.util.Collections;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

public class ClusterTimeGenerator extends ServerTimeGenerator {

  private MetaGroupMember metaGroupMember;

  /**
   * Constructor of EngineTimeGenerator.
   */
  public ClusterTimeGenerator(IExpression expression,
      QueryContext context, MetaGroupMember metaGroupMember) throws StorageEngineException {
    super(context);
    this.metaGroupMember = metaGroupMember;
    try {
      constructNode(expression);
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    Filter filter = expression.getFilter();
    Path path = expression.getSeriesPath();
    TSDataType dataType;
    try {
      dataType =
          metaGroupMember.getSeriesTypesByString(Collections.singletonList(path.getFullPath()),
              null).get(0);
      return metaGroupMember.getSeriesReader(path, dataType, null, filter, context);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
