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
import org.apache.iotdb.backup.core.pipeline.context.model.CompressEnum;
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
import java.util.stream.Collectors;

public class OutCompressDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutCompressDataSource.class);

  private String name;

  private Scheduler scheduler;

  private ConcurrentHashMap<String, OutputStream> COMPRESS_PRINTER_MAP = new ConcurrentHashMap<>();

  private static final String CATALOG_COMPRESS = "CATALOG_COMPRESS.CATALOG";

  private AtomicLong fileNo = new AtomicLong();

  private ExportPipelineService exportPipelineService;

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s -> {
                  String catalog = CATALOG_COMPRESS;
                  return exportPipelineService.parseFileSinkStrategy(COMPRESS_PRINTER_MAP, catalog);
                })
            .flatMap(s -> exportPipelineService.countDeviceNum(s, totalSize))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(exportPipelineService::parseTimeseries)
            .flatMap(s -> this.initOutputStream(s, fileNo, COMPRESS_PRINTER_MAP))
            .flatMap(exportPipelineService::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  try {
                    for (String key : COMPRESS_PRINTER_MAP.keySet()) {
                      COMPRESS_PRINTER_MAP.get(key).flush();
                      COMPRESS_PRINTER_MAP.get(key).close();
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  context = context.put("totalSize", totalSize);
                  context = context.put("outputStreamMap", COMPRESS_PRINTER_MAP);
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
          try {
            String fileName;
            OutputStream out;
            long no = fileNo.incrementAndGet();
            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              fileName = exportModel.getFileFolder() + no;
            } else {
              fileName = exportModel.getFileFolder() + pair.getLeft().getDeviceName();
            }

            if (exportModel.getCompressEnum() == CompressEnum.SNAPPY) {
              fileName = fileName + ".snappy.bin";
            } else if (exportModel.getCompressEnum() == CompressEnum.GZIP) {
              fileName = fileName + ".gz.bin";
            } else {
              fileName = fileName + ".lz4.bin";
            }
            out = new FileOutputStream(new File(fileName));

            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              StringBuilder catalogRecord = new StringBuilder();
              catalogRecord
                  .append(no)
                  .append(",")
                  .append(pair.getLeft().getDeviceName())
                  .append("\r\n");
              outputStreamMap.get("CATALOG").write(catalogRecord.toString().getBytes());
            }
            List<String> timeseriesNameList =
                pair.getRight().stream().map(TimeseriesModel::getName).collect(Collectors.toList());
            exportPipelineService.compressHeader(timeseriesNameList, out, exportModel);
            outputStreamMap.put(String.valueOf(pair.getLeft().getDeviceName()), out);
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.just(pair);
        });
  }

  public OutCompressDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public OutCompressDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    this.scheduler = Schedulers.newParallel("compress-pipeline-thread", this.parallelism);
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
