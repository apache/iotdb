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

package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractDataSourceOperator extends AbstractSourceOperator
    implements DataSourceOperator {

  protected SeriesScanUtil seriesScanUtil;

  protected AtomicBoolean finished = new AtomicBoolean(false);

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {
    seriesScanUtil.initQueryDataSource(dataSource);
  }

  @Override
  public boolean isFinished() {
    return finished.get();
  }

  public void setSeriesScanUtil(SeriesScanUtil seriesScanUtil) {
    this.seriesScanUtil = seriesScanUtil;
  }

  public void setFinished(boolean finished) {
    this.finished.set(finished);
  }
}
