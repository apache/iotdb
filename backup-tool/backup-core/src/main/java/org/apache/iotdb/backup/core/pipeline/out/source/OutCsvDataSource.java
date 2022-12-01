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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OutCsvDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutCsvDataSource.class);

  private String name;

  private Scheduler scheduler;

  private ConcurrentHashMap<String, CSVPrinter> CSV_PRINTER_MAP = new ConcurrentHashMap<>();

  private OutputStream[] outputStreams = new OutputStream[1];

  private static final String CATALOG_CSV = "CATALOG_CSV.CATALOG";

  private AtomicLong fileNo = new AtomicLong();

  private ExportPipelineService exportPipelineService;

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(s -> this.parseFileSinkStrategy(outputStreams, CATALOG_CSV))
            .flatMap(s -> exportPipelineService.countDeviceNum(s, totalSize))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(exportPipelineService::parseTimeseries)
            .flatMap(s -> this.initOutputStream(s, fileNo, CSV_PRINTER_MAP, outputStreams[0]))
            .flatMap(exportPipelineService::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  try {
                    if (outputStreams[0] != null) {
                      outputStreams[0].close();
                    }
                    for (String key : CSV_PRINTER_MAP.keySet()) {
                      CSV_PRINTER_MAP.get(key).flush();
                      CSV_PRINTER_MAP.get(key).close();
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  context = context.put("totalSize", totalSize);
                  context = context.put("outputStreamMap", CSV_PRINTER_MAP);
                  return context;
                });
  }

  /**
   * 导出文件命名规则， PATH_FILENAME 以设备实体path为文件名，缺点 pc路径长度限制，pc文件名特殊字符冲突 EXTRA_CATALOG
   * 单独生成一个目录文档，文件名为数字递增，文档中记录设备实体path与文件名对应关系 本方法：生成目录文档的outputstream并放入outputStreamMap中
   *
   * @param outputStream
   * @param catalogName
   * @return
   */
  public Flux<String> parseFileSinkStrategy(OutputStream[] outputStream, String catalogName) {
    return Flux.deferContextual(
        context -> {
          PipelineContext<ExportModel> pcontext = context.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
            File file = new File(exportModel.getFileFolder());
            if (!file.exists()) {
              file.mkdirs();
            }
            String catalogFilePath = exportModel.getFileFolder() + catalogName;
            try {
              outputStream[0] = new FileOutputStream(catalogFilePath);
              String header = "FILE_NAME,ENTITY_PATH\r\n";
              outputStream[0].write(header.getBytes());
            } catch (IOException e) {
              log.error("异常信息:", e);
            }
          }
          return Flux.just(catalogName);
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
      ConcurrentHashMap<String, CSVPrinter> outputStreamMap,
      OutputStream catalogOutStream) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          FileOutputStream outputStream = null;
          try {
            String fileName;
            long no = fileNo.incrementAndGet();
            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              fileName = exportModel.getFileFolder() + no + ".csv";
            } else {
              fileName = exportModel.getFileFolder() + pair.getLeft().getDeviceName() + ".csv";
            }
            if (exportModel.getFileSinkStrategyEnum() == FileSinkStrategyEnum.EXTRA_CATALOG) {
              StringBuilder catalogRecord = new StringBuilder();
              catalogRecord
                  .append(no)
                  .append(",")
                  .append(pair.getLeft().getDeviceName())
                  .append("\r\n");
              catalogOutStream.write(catalogRecord.toString().getBytes());
            }
            outputStream = new FileOutputStream(new File(fileName));
            List<String> headerList = new ArrayList<>();
            headerList.add("Time");
            List<String> timeseriesNameList =
                pair.getRight().stream().map(TimeseriesModel::getName).collect(Collectors.toList());
            headerList.addAll(timeseriesNameList);
            CSVPrinter printer =
                CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setEscape('\\')
                    .setQuoteMode(QuoteMode.NONE)
                    .build()
                    .print(new OutputStreamWriter(outputStream, exportModel.getCharSet()));
            printer.printRecord(headerList);
            printer.flush();
            outputStreamMap.put(String.valueOf(pair.getLeft().getDeviceName()), printer);
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.just(pair);
        });
  }

  public OutCsvDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public OutCsvDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    this.scheduler = Schedulers.newParallel("csv-pipeline-thread", this.parallelism);
    // TODO： 是否需要单例
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
