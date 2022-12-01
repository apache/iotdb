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
import org.apache.iotdb.backup.core.pipeline.context.model.FileSinkStrategyEnum;
import org.apache.iotdb.backup.core.service.ExportPipelineService;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class OutSqlDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutSqlDataSource.class);

  private String name;

  private Scheduler scheduler;

  private ConcurrentHashMap<String, OutputStream> OUTPUT_STREAM_MAP = new ConcurrentHashMap<>();

  private static final String CATALOG_SQL = "CATALOG_SQL.CATALOG";

  private AtomicLong fileNo = new AtomicLong();

  private ExportPipelineService exportPipelineService;

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s -> exportPipelineService.parseFileSinkStrategy(OUTPUT_STREAM_MAP, CATALOG_SQL))
            .flatMap(s -> exportPipelineService.countDeviceNum(s, totalSize))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(exportPipelineService::parseTimeseries)
            .flatMap(s -> this.initOutputStream(s, fileNo, OUTPUT_STREAM_MAP))
            .flatMap(exportPipelineService::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  try {
                    for (String key : OUTPUT_STREAM_MAP.keySet()) {
                      OUTPUT_STREAM_MAP.get(key).flush();
                      OUTPUT_STREAM_MAP.get(key).close();
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  context = context.put("totalSize", totalSize);
                  context = context.put("outputStreamMap", OUTPUT_STREAM_MAP);
                  return context;
                });
  }

  /**
   * 初始化设备实体对应的outputStream
   *
   * @param pair
   * @return
   */
  public Flux<Pair<DeviceModel, List<TimeseriesModel>>> initOutputStream(
      Pair<DeviceModel, List<TimeseriesModel>> pair,
      AtomicLong fileNo,
      ConcurrentHashMap<String, OutputStream> outputStreamMap) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          FileOutputStream outputStream = null;
          try {
            File file = new File(exportModel.getFileFolder());
            String fileName;
            long no = fileNo.incrementAndGet();
            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              fileName = exportModel.getFileFolder() + no;
            } else {
              fileName = exportModel.getFileFolder() + pair.getLeft().getDeviceName();
            }
            fileName = fileName + ".sql";
            file = new File(fileName);
            outputStream = new FileOutputStream(file);
            outputStreamMap.put(String.valueOf(pair.getLeft().getDeviceName()), outputStream);
            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              StringBuilder catalogRecord = new StringBuilder();
              catalogRecord
                  .append(no)
                  .append(",")
                  .append(pair.getLeft().getDeviceName())
                  .append("\r\n");
              outputStreamMap.get("CATALOG").write(catalogRecord.toString().getBytes());
            }
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.just(pair);
        });
  }

  public OutSqlDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public OutSqlDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    scheduler = Schedulers.newParallel("pipeline-thread", this.parallelism);
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
