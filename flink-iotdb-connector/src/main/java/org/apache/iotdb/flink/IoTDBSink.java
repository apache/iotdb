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

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The `IoTDBSink` allows flink jobs to write events into IoTDB timeseries.
 * By default send only one event after another, but you can change to batch by invoking `withBatchSize(int)`.
 * @param <IN> input type
 */
public class IoTDBSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IoTDBSink.class);

    private IoTSerializationSchema<IN> serializationSchema;
    private IoTDBOptions options;
    private transient Session session;
    private transient ScheduledExecutorService scheduledExecutor;

    private int batchSize = 0;
    private int flushIntervalMs = 3000;
    private transient List<Event> batchList;

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
        for (IoTDBOptions.TimeseriesOption option : options.getTimeseriesOptionList()) {
            if (!session.checkTimeseriesExists(option.getPath())) {
                session.createTimeseries(option.getPath(), option.getDataType(), option.getEncoding(), option.getCompressor());
            }
        }

        if (batchSize > 0) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutor.scheduleAtFixedRate(() -> {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.error("flush error", e);
                }
            }, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    //  for testing
    void setSession(Session session) {
        this.session = session;
    }

    void setBatchList(List<Event> batchList) {
        this.batchList = batchList;
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        Event event = serializationSchema.serialize(input);
        if (event == null) {
            return;
        }

        if (batchSize > 0) {
            batchList.add(event);
            if (batchList.size() >= batchSize) {
                flush();
            }
            return;
        }

        TSStatus status = session.insert(event.getDevice(), event.getTimestamp(),
                event.getMeasurements(), event.getValues());
        LOG.debug("send event result: {}", status);
    }

    public IoTDBSink<IN> withBatchSize(int batchSize) {
        Preconditions.checkArgument(batchSize >= 0);
        this.batchSize = batchSize;
        return this;
    }

    public IoTDBSink<IN> withFlushIntervalMs(int flushIntervalMs) {
        Preconditions.checkArgument(flushIntervalMs > 0);
        this.flushIntervalMs = flushIntervalMs;
        return this;
    }

    @Override
    public void close() throws Exception {
        if (session != null) {
            try {
                flush();
            } catch (Exception e) {
                LOG.error("flush error", e);
            }
            session.close();
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
    }

    private synchronized void flush() throws Exception {
        if (batchSize > 0 && batchList.size() > 0) {
            List<String> deviceIds = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<String>> valuesList = new ArrayList<>();

            for (Event event : batchList) {
                deviceIds.add(event.getDevice());
                timestamps.add(event.getTimestamp());
                measurementsList.add(event.getMeasurements());
                valuesList.add(event.getValues());
            }
            List<TSStatus> statusList = session.insertInBatch(deviceIds, timestamps, measurementsList, valuesList);
            LOG.debug("send events result: {}", statusList);
            batchList.clear();
        }
    }

}
