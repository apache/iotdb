/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.handlers.caller;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullTimeseriesSchemaHandler implements AsyncMethodCallback<PullSchemaResp> {

  private static final Logger logger = LoggerFactory.getLogger(PullTimeseriesSchemaHandler.class);

  private Node node;
  private String prefixPath;
  private AtomicReference<List<MeasurementSchema>> timeseriesSchemas;

  public PullTimeseriesSchemaHandler(Node node, String prefixPath,
      AtomicReference<List<MeasurementSchema>> timeseriesSchemas) {
    this.node = node;
    this.prefixPath = prefixPath;
    this.timeseriesSchemas = timeseriesSchemas;
  }

  @Override
  public void onComplete(PullSchemaResp response) {
    ByteBuffer buffer = response.schemaBytes;
    int size = buffer.getInt();
    List<MeasurementSchema> schemas = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      schemas.add(MeasurementSchema.deserializeFrom(buffer));
    }
    synchronized (timeseriesSchemas) {
      timeseriesSchemas.set(schemas);
      timeseriesSchemas.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot pull time series schema of {} from {}", prefixPath, node, exception);
    synchronized (timeseriesSchemas) {
      timeseriesSchemas.notifyAll();
    }
  }
}
