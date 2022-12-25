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
package org.apache.iotdb.db.metadata.schemaregion.rocksdb;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateTimeSeriesPlan;

import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Ignore
public class MRocksDBBenchmark {
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final RSchemaRegion rocksDBManager;

  private static final Logger logger = LoggerFactory.getLogger(MRocksDBBenchmark.class);

  public MRocksDBBenchmark(RSchemaRegion rocksDBManager) {
    this.rocksDBManager = rocksDBManager;
  }

  public List<RocksDBBenchmarkTask.BenchmarkResult> benchmarkResults = new ArrayList<>();

  public void testTimeSeriesCreation(List<List<ICreateTimeSeriesPlan>> timeSeriesSet)
      throws IOException {
    RocksDBBenchmarkTask<List<ICreateTimeSeriesPlan>> task =
        new RocksDBBenchmarkTask<>(timeSeriesSet, RocksDBTestUtils.WRITE_CLIENT_NUM, 100);
    RocksDBBenchmarkTask.BenchmarkResult result =
        task.runBatchWork(
            createTimeSeriesPlans -> {
              RocksDBBenchmarkTask.TaskResult taskResult = new RocksDBBenchmarkTask.TaskResult();
              createTimeSeriesPlans.forEach(
                  ts -> {
                    try {
                      rocksDBManager.createTimeseries(
                          ts.getPath(),
                          ts.getDataType(),
                          ts.getEncoding(),
                          ts.getCompressor(),
                          ts.getProps(),
                          ts.getAlias());
                      taskResult.success++;
                    } catch (Exception e) {
                      e.printStackTrace();
                      taskResult.failure++;
                    }
                  });
              return taskResult;
            },
            "CreateTimeSeries");
    benchmarkResults.add(result);
  }

  public void testMeasurementNodeQuery(Collection<String> queryTsSet) {
    RocksDBBenchmarkTask<String> task =
        new RocksDBBenchmarkTask<>(queryTsSet, RocksDBTestUtils.WRITE_CLIENT_NUM, 10000);
    RocksDBBenchmarkTask.BenchmarkResult result =
        task.runWork(
            s -> {
              try {
                IMNode node = rocksDBManager.getMeasurementMNode(new PartialPath(s));
                if (node != null) {
                  logger.warn(node.toString());
                }
                return true;
              } catch (Exception e) {
                return false;
              }
            },
            "MeasurementNodeQuery");
    benchmarkResults.add(result);
  }
}
