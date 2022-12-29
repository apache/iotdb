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
package org.apache.iotdb.backup.core.pipeline.out.sink;

import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.service.ExportPipelineService;

import org.apache.commons.csv.CSVPrinter;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OutStructureFileSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

  private static final Logger log = LoggerFactory.getLogger(OutStructureFileSink.class);

  private String name;

  private ExportPipelineService exportPipelineService;

  // 数据条数
  private AtomicInteger finishedFileNum = new AtomicInteger();

  private AtomicLong finishedRowNum = new AtomicLong();

  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return sink ->
        sink.transformGroups(
                (Function<
                        GroupedFlux<Integer, TimeSeriesRowModel>,
                        Publisher<? extends TimeSeriesRowModel>>)
                    integerTimeSeriesRowModelGroupedFlux ->
                        integerTimeSeriesRowModelGroupedFlux
                            .buffer(1000, 1000)
                            .flatMap(
                                allList -> {
                                  return Flux.deferContextual(
                                      contextView -> {
                                        CSVPrinter[] csvPrinters = contextView.get("csvPrinters");
                                        CSVPrinter printer = csvPrinters[0];
                                        List<List<String>> list =
                                            allList.stream()
                                                .map(s -> generateCsvString(s))
                                                .collect(Collectors.toList());
                                        try {
                                          exportPipelineService.syncPrintRecoreds(printer, list);
                                          printer.flush();
                                        } catch (IOException e) {
                                          log.error("异常信息:", e);
                                        }
                                        finishedRowNum.addAndGet(allList.size());
                                        return Flux.fromIterable(allList);
                                      });
                                }))
            .doOnTerminate(
                () -> {
                  finishedFileNum.incrementAndGet();
                });
  }

  @Override
  public Double[] rateOfProcess() {
    log.info("已经导出文件：{}", finishedFileNum);
    log.info("总文件数：{}", 1);
    Double[] rateDouble = new Double[2];
    rateDouble[0] = finishedFileNum.doubleValue();
    rateDouble[1] = 1d;
    return rateDouble;
  }

  @Override
  public Long finishedRowNum() {
    return finishedRowNum.get();
  }

  public List<String> generateCsvString(TimeSeriesRowModel timeSeriesRowModel) {
    List<String> value = new ArrayList<>();
    for (int i = 0; i < timeSeriesRowModel.getIFieldList().size(); i++) {
      IField iField = timeSeriesRowModel.getIFieldList().get(i);
      if (iField.getField() != null
          && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
        String valueStr =
            String.valueOf(iField.getField().getObjectValue(iField.getField().getDataType()));
        value.add(valueStr);
      } else {
        value.add("");
      }
    }

    value.add(
        new StringBuilder()
            .append("\"")
            .append(timeSeriesRowModel.getDeviceModel().isAligned())
            .append("\"")
            .toString());
    return value;
  }

  public OutStructureFileSink(String name) {
    this.name = name;
    if (exportPipelineService == null) {
      exportPipelineService = ExportPipelineService.exportPipelineService();
    }
  }
}
