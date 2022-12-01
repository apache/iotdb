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
import org.apache.iotdb.backup.core.model.FieldCopy;
import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class OutStructureSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(OutStructureSource.class);

  private String name;

  private Scheduler scheduler = Schedulers.newParallel("structure-pipeline-thread", 1);

  private static final String TIMESERIES_STRUCTURE = "TIMESERIES_STRUCTURE.STRUCTURE";

  private ExportPipelineService exportPipelineService;

  private CSVPrinter[] csvPrinters = new CSVPrinter[1];

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(s -> this.generateCsvPrinter(TIMESERIES_STRUCTURE))
            .flatMap(s -> exportPipelineService.parseToDeviceModel())
            .parallel(1)
            .runOn(scheduler)
            .flatMap(this::parseToRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  try {
                    for (CSVPrinter printer : csvPrinters) {
                      printer.flush();
                      printer.close();
                    }
                    scheduler.dispose();
                  } catch (IOException e) {
                    log.error("异常信息:", e);
                  }
                })
            .contextWrite(
                context -> {
                  return context.put("csvPrinters", csvPrinters);
                });
  }

  public Flux<String> generateCsvPrinter(String structureFileName) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          try {
            File file = new File(exportModel.getFileFolder());
            if (!file.exists()) {
              file.mkdirs();
            }
            FileOutputStream outputStream =
                new FileOutputStream(exportModel.getFileFolder() + TIMESERIES_STRUCTURE);
            CSVPrinter printer =
                CSVFormat.Builder.create(CSVFormat.DEFAULT)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setEscape('\\')
                    .setQuoteMode(QuoteMode.NONE)
                    .build()
                    .print(new OutputStreamWriter(outputStream, exportModel.getCharSet()));
            List<String> headerList = new ArrayList<>();
            headerList.add("timeseries");
            headerList.add("alias");
            headerList.add("storage group");
            headerList.add("dataType");
            headerList.add("encoding");
            headerList.add("compression");
            headerList.add("tags");
            headerList.add("attributes");
            headerList.add("aligned");
            exportPipelineService.syncPrint(printer, headerList);
            csvPrinters[0] = printer;
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.just(structureFileName);
        });
  }

  /**
   * 通过entity 解析出其对应的entity实体,并把实体对应的row作为stream流 show timeseries 返回的record里面的类型全部是txt
   *
   * @param deviceModel
   * @return
   */
  public Flux<TimeSeriesRowModel> parseToRowModel(DeviceModel deviceModel) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ExportModel> pcontext = contextView.get("pipelineContext");
          ExportModel exportModel = pcontext.getModel();
          String version = contextView.get("VERSION");
          return Flux.create(
              (Consumer<FluxSink<TimeSeriesRowModel>>)
                  sink -> {
                    Session session = exportModel.getSession();
                    StringBuilder buffer = new StringBuilder();
                    buffer
                        .append("show timeseries ")
                        .append(
                            ExportPipelineService.formatPath(deviceModel.getDeviceName(), version))
                        .append(".*");
                    String sql = buffer.toString();
                    try {
                      SessionDataSet timeseriesData = session.executeQueryStatement(sql);
                      List<String> cloumnNameList = timeseriesData.getColumnNames();
                      while (timeseriesData.hasNext()) {
                        TimeSeriesRowModel rowModel = new TimeSeriesRowModel();
                        rowModel.setDeviceModel(deviceModel);
                        RowRecord record = timeseriesData.next();
                        List<IField> iFieldList = new ArrayList<>();
                        for (String name : cloumnNameList) {
                          IField iField = new IField();
                          iField.setColumnName(name);
                          iField.setField(
                              FieldCopy.copy(record.getFields().get(cloumnNameList.indexOf(name))));
                          iFieldList.add(iField);
                        }
                        rowModel.setIFieldList(iFieldList);
                        sink.next(rowModel);
                      }
                      sink.complete();
                    } catch (StatementExecutionException | IoTDBConnectionException e) {
                      sink.error(e);
                    }
                  });
        });
  }

  public OutStructureSource(String name) {
    this.name = name;
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
