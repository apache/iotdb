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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Map;

public class TemplatedConcatDeviceAndPredicateVisitor
    extends ReconstructVisitor<TemplatedConcatDeviceAndPredicateVisitor.Context> {

  @Override
  public Expression visitTimeSeriesOperand(
      TimeSeriesOperand predicate, TemplatedConcatDeviceAndPredicateVisitor.Context context) {
    PartialPath measurementPath = predicate.getPath();
    PartialPath concatPath = context.getDevicePath().concatPath(measurementPath);
    IMeasurementSchema schema = null;
    if (context.getSchemaMap().containsKey(measurementPath.getMeasurement())) {
      schema = context.getSchemaMap().get(measurementPath.getMeasurement());
      MeasurementPath fullMeasurementPath = new MeasurementPath(concatPath, schema);
      return new TimeSeriesOperand(fullMeasurementPath);
    } else {
      return new NullOperand();
    }
  }

  public static class Context {
    private final PartialPath devicePath;

    private final Map<String, IMeasurementSchema> schemaMap;

    public Context(PartialPath devicePath, Map<String, IMeasurementSchema> schemaMap) {
      this.devicePath = devicePath;
      this.schemaMap = schemaMap;
    }

    public PartialPath getDevicePath() {
      return devicePath;
    }

    public Map<String, IMeasurementSchema> getSchemaMap() {
      return this.schemaMap;
    }
  }
}
