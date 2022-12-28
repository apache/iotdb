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
package org.apache.iotdb.backup.core.pipeline.out.source;

import org.apache.iotdb.backup.core.model.DeviceModel;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.model.TimeseriesModel;
import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** @Author: LL @Description: @Date: create in 2022/11/24 17:11 */
public class OutTsfileDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutTsfileDataSource.class);

  private String name;

  private Scheduler scheduler;

  private ConcurrentHashMap<String, Schema> SCHEMA_MAP = new ConcurrentHashMap<>();

  private ConcurrentHashMap<String, TsFileWriter> TSFILE_WRITER_MAP = new ConcurrentHashMap<>();

  private ConcurrentHashMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap =
      new ConcurrentHashMap<>();

  private ExportPipelineService exportPipelineService;

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(s -> this.validateStorage(s))
            .flatMap(s -> exportPipelineService.countDeviceNum(s, totalSize))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(exportPipelineService::parseTimeseries)
            .flatMap(s -> generateDeviceInfoMap(s, deviceInfoMap))
            .flatMap(exportPipelineService::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doOnComplete(
                () -> {
                  try {
                    for (String key : TSFILE_WRITER_MAP.keySet()) {
                      TSFILE_WRITER_MAP.get(key).flushAllChunkGroups();
                      TSFILE_WRITER_MAP.get(key).close();
                      TSFILE_WRITER_MAP.remove(key);
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  context = context.put("totalSize", totalSize);
                  context = context.put("tsfileWriterMap", TSFILE_WRITER_MAP);
                  context = context.put("deviceInfoMap", deviceInfoMap);
                  context = context.put("schemaMap", SCHEMA_MAP);
                  return context;
                });
  }

  private Flux<String> validateStorage(String s) {
    return Flux.deferContextual(
        context -> {
          PipelineContext<ExportModel> pcontext = context.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          String version = context.get("VERSION");
          StringBuilder sqlBuilder = new StringBuilder();
          sqlBuilder
              .append("count storage group ")
              .append(ExportPipelineService.formatPath(exportModel.getIotdbPath(), version));
          String sql = sqlBuilder.toString();
          try {
            SessionDataSet deviceData = exportModel.getSession().executeQueryStatement(sql);
            if (deviceData.hasNext()) {
              RowRecord rowRecord = deviceData.next();
              Integer count =
                  rowRecord
                      .getFields()
                      .get(deviceData.getColumnNames().indexOf("storage group"))
                      .getIntV();
              if (count > 1) {
                throw new Exception("the path has more than one storage group");
              }
            }
          } catch (StatementExecutionException | IoTDBConnectionException e) {
            log.error("异常信息sql:{},e : {}", sql, e);
          } catch (Exception e) {
            log.error("异常信息sql:{},e : {}", sql, e);
            return Flux.error(e);
          }
          return Flux.just(s);
        });
  }

  private Flux<Pair<DeviceModel, List<TimeseriesModel>>> generateDeviceInfoMap(
      Pair<DeviceModel, List<TimeseriesModel>> pair,
      ConcurrentHashMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap) {
    return Flux.deferContextual(
        contextView -> {
          deviceInfoMap.put(pair.getLeft().getDeviceName(), pair);
          return Flux.just(pair);
        });
  }

  public OutTsfileDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public OutTsfileDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    scheduler = Schedulers.newParallel("pipeline-thread", this.parallelism);
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
