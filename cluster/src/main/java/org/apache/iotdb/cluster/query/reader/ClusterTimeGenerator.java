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
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

public class ClusterTimeGenerator extends ServerTimeGenerator {

  private ClusterReaderFactory readerFactory;

  /**
   * Constructor of EngineTimeGenerator.
   */
  public ClusterTimeGenerator(IExpression expression,
      QueryContext context, MetaGroupMember metaGroupMember,
      RawDataQueryPlan rawDataQueryPlan) throws StorageEngineException {
    super(context);
    this.queryPlan = rawDataQueryPlan;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
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
    PartialPath path = (PartialPath) expression.getSeriesPath();
    TSDataType dataType;
    try {
      dataType =
          ((CMManager) IoTDB.metaManager).getSeriesTypesByPaths(Collections.singletonList(path),
              null).left.get(0);
      return readerFactory.getSeriesReader(path,
          queryPlan.getAllMeasurementsInDevice(path.getDevice()), dataType,
          null, filter, context, queryPlan.isAscending());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
