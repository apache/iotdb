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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PullMeasurementSchemaHandler implements AsyncMethodCallback<PullSchemaResp> {

  private static final Logger logger = LoggerFactory.getLogger(PullMeasurementSchemaHandler.class);

  private Node owner;
  private List<String> prefixPaths;
  private AtomicReference<List<IMeasurementSchema>> timeseriesSchemas;

  public PullMeasurementSchemaHandler(
      Node owner,
      List<String> prefixPaths,
      AtomicReference<List<IMeasurementSchema>> timeseriesSchemas) {
    this.owner = owner;
    this.prefixPaths = prefixPaths;
    this.timeseriesSchemas = timeseriesSchemas;
  }

  @Override
  public void onComplete(PullSchemaResp response) {
    ByteBuffer buffer = response.schemaBytes;
    int size = buffer.getInt();
    List<IMeasurementSchema> schemas = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      schemas.add(
          buffer.get() == 0
              ? UnaryMeasurementSchema.partialDeserializeFrom(buffer)
              : VectorMeasurementSchema.partialDeserializeFrom(buffer));
    }
    synchronized (timeseriesSchemas) {
      timeseriesSchemas.set(schemas);
      timeseriesSchemas.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot pull time series schema of {} from {}", prefixPaths, owner, exception);
    synchronized (timeseriesSchemas) {
      timeseriesSchemas.notifyAll();
    }
  }
}
