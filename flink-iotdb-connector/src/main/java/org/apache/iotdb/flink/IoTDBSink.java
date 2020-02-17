/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink;

import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class IoTDBSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSink.class);

    private IoTSerializationSchema<IN> serializationSchema;
    private IoTDBOptions options;
    private transient Session session;

    private boolean batchFlushOnCheckpoint; // false by default
    private int batchSize = 100;
    private List<Event> batchList;

    public IoTDBSink(IoTDBOptions options, IoTSerializationSchema<IN> schema) {
        this.options = options;
        this.serializationSchema = schema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        batchList = new LinkedList<>();

        session = new Session(options.getHost(), options.getPort(), options.getUser(), options.getPassword());
        session.open();

        session.setStorageGroup(options.getStorageGroup());
        for (String sensor : options.getTimeseries()) {
            session.createTimeseries(sensor, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        }

        if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
            batchFlushOnCheckpoint = false;
        }
    }

    //  for testing
    void setSession(Session session) {
        this.session = session;
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        Event event = serializationSchema.serialize(input);
        if (event == null) {
            return;
        }

        if (batchFlushOnCheckpoint) {
            batchList.add(event);
            if (batchList.size() >= batchSize) {
                flushSync();
            }
            return;
        }

        TSStatus status = session.insert(event.getDevice(), event.getTimestamp(),
                Lists.newArrayList(event.getMeasurement()), Lists.newArrayList(event.getValue()));
        LOG.debug("sync send event result: {}", status);
    }

    public IoTDBSink<IN> withBatchFlushOnCheckpoint(boolean batchFlushOnCheckpoint) {
        this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
        return this;
    }

    public IoTDBSink<IN> withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            try {
                flushSync();
            } catch (Exception e) {
                LOG.error("flushSync failure", e);
            }
           session.close();
        }
    }

    private void flushSync() throws Exception {
        if (batchFlushOnCheckpoint) {
            synchronized (batchList) {
                if (batchList.size() > 0) {
                    List<String> deviceIds = new ArrayList<>();
                    List<Long> timestamps = new ArrayList<>();
                    List<List<String>> measurementsList = new ArrayList<>();
                    List<List<String>> valuesList = new ArrayList<>();

                    for (Event event : batchList) {
                        deviceIds.add(event.getDevice());
                        timestamps.add(event.getTimestamp());
                        measurementsList.add(Lists.newArrayList(event.getMeasurement()));
                        valuesList.add(Lists.newArrayList(event.getValue()));
                    }
                    List<TSStatus> statusList = session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
                    LOG.debug("sync send events result: {}", statusList);
                    batchList.clear();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushSync();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }
}
