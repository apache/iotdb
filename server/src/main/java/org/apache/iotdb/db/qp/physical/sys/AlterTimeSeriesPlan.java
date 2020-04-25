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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AlterTimeSeriesPlan extends PhysicalPlan {

  private Path path;

  private AlterTimeSeriesOperator.AlterType alterType;

  private Map<String, String> alterMap;

  public AlterTimeSeriesPlan(Path path, AlterTimeSeriesOperator.AlterType alterType, Map<String, String> alterMap) {
    super(false, Operator.OperatorType.ALTER_TIMESERIES);
    this.path = path;
    this.alterType = alterType;
    this.alterMap = alterMap;
  }

  public Path getPath() {
    return path;
  }

  public AlterTimeSeriesOperator.AlterType getAlterType() {
    return alterType;
  }

  public Map<String, String> getAlterMap() {
    return alterMap;
  }

  @Override
  public List<Path> getPaths() {
    return Collections.singletonList(path);
  }
}
