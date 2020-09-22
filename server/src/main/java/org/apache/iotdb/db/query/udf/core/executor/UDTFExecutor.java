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

package org.apache.iotdb.db.query.udf.core.executor;

import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Point;
import org.apache.iotdb.db.query.udf.api.access.PointWindow;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;
import org.apache.iotdb.db.query.udf.datastructure.tv.ElasticSerializableTVList;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;

public class UDTFExecutor {

  protected final UDTFConfigurations configurations;
  protected final UDTF udtf;
  protected final ElasticSerializableTVList collector;

  public UDTFExecutor(long queryId, UDFContext context) throws Exception {
    configurations = new UDTFConfigurations();
    udtf = (UDTF) UDFRegistrationService.getInstance().reflect(context);
    udtf.beforeStart(new UDFParameters(context.getPaths(), context.getDataTypes(),
        context.getAttributes()), configurations);
    configurations.check();
    collector = new ElasticSerializableTVList(configurations.getOutputDataType(), queryId,
        context.getColumnName(), ElasticSerializableTVList.DEFAULT_MEMORY_USAGE_LIMIT,
        ElasticSerializableTVList.DEFAULT_CACHE_SIZE);
  }

  public void execute(Point point) throws Exception {
    udtf.transform(point);
  }

  public void execute(PointWindow pointWindow) throws Exception {
    udtf.transform(pointWindow);
  }

  public void execute(Row row) throws Exception {
    udtf.transform(row);
  }

  public void execute(RowWindow rowWindow) throws Exception {
    udtf.transform(rowWindow);
  }

  public void beforeDestroy() {
    udtf.beforeDestroy();
  }

  public UDTFConfigurations getConfigurations() {
    return configurations;
  }

  public ElasticSerializableTVList getCollector() {
    return collector;
  }
}
