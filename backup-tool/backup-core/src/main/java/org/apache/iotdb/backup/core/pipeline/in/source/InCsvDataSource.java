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
package org.apache.iotdb.backup.core.pipeline.in.source;

import org.apache.iotdb.backup.core.exception.ParamCheckException;
import org.apache.iotdb.backup.core.model.DeviceModel;
import org.apache.iotdb.backup.core.model.FieldCopy;
import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.backup.core.service.ImportPipelineService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class InCsvDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(InCsvDataSource.class);

  private String name;

  private ImportPipelineService importPipelineService;

  private Scheduler scheduler;

  private static final String CATALOG_CSV = "CATALOG_CSV.CATALOG";

  private ConcurrentHashMap<String, List<InputStream>> COMPRESS_MAP = new ConcurrentHashMap<>();

  private Integer[] totalSize = new Integer[1];

  private int parallelism;

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s ->
                    Flux.deferContextual(
                        contextView -> {
                          PipelineContext<ImportModel> context = contextView.get("pipelineContext");
                          ImportModel importModel = context.getModel();
                          FilenameFilter filenameFilter = initFileFilter(importModel);
                          totalSize[0] =
                              importPipelineService.getFileArray(
                                      filenameFilter, importModel.getFileFolder())
                                  .length;
                          return importPipelineService.parseFluxFileName(
                              filenameFilter, COMPRESS_MAP);
                        }))
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(this::parseTimeSeriesRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  for (String key : COMPRESS_MAP.keySet()) {
                    COMPRESS_MAP
                        .get(key)
                        .forEach(
                            inputStream -> {
                              if (inputStream != null) {
                                try {
                                  inputStream.close();
                                } catch (IOException e) {
                                }
                              }
                            });
                  }
                  scheduler.dispose();
                })
            .contextWrite(
                context -> {
                  return context.put("totalSize", totalSize);
                });
  }

  /**
   * 读取csv文件，把数据转化为TimeSeriesRowModel流 fileHeaderMap timeseries和csv中所在position的map tsDataTypeMap
   * timeseries和TSDataType的map 根据timeseries获取数据对应的类型以及其对应的值 record.get(fileHeaderMap.get(header))
   *
   * @param in
   * @return
   */
  public Flux<TimeSeriesRowModel> parseTimeSeriesRowModel(InputStream in) {
    return Flux.deferContextual(
        context -> {
          PipelineContext<ImportModel> pcontext = context.get("pipelineContext");
          ImportModel importModel = pcontext.getModel();
          String version = context.get("VERSION");
          return Flux.create(
              (Consumer<FluxSink<TimeSeriesRowModel>>)
                  sink -> {
                    try {
                      CSVParser parser =
                          CSVFormat.Builder.create(CSVFormat.DEFAULT)
                              .setHeader()
                              .setSkipHeaderRecord(true)
                              .setQuote('\\')
                              .setEscape('\\')
                              .setIgnoreEmptyLines(true)
                              .setQuoteMode(QuoteMode.NONE)
                              .build()
                              .parse(new InputStreamReader(in, importModel.getCharSet()));
                      Iterator<CSVRecord> it = parser.iterator();

                      Map<String, Integer> fileHeaderMap = parser.getHeaderMap();
                      String entityPath = null;
                      for (String header : fileHeaderMap.keySet()) {
                        if ("Time".equals(header)) {
                          continue;
                        } else {
                          entityPath = header.substring(0, header.lastIndexOf("."));
                        }
                      }
                      // 存在entitypath为空的情况,也就是csv中只有time一列的时候
                      if (entityPath != null) {
                        StringBuilder sql = new StringBuilder();
                        sql.append(" show devices ")
                            .append(ExportPipelineService.formatPath(entityPath, version));
                        SessionDataSet alignedSet =
                            importModel.getSession().executeQueryStatement(sql.toString());

                        List<String> columnNameList = alignedSet.getColumnNames();
                        DeviceModel deviceModel = new DeviceModel();
                        if (alignedSet.hasNext()) {
                          RowRecord record = alignedSet.next();
                          int position = columnNameList.indexOf("isAligned");
                          String aligned = "false";
                          if (position != -1) {
                            aligned = record.getFields().get(position).getStringValue();
                          }
                          deviceModel.setDeviceName(entityPath);
                          deviceModel.setAligned(Boolean.parseBoolean(aligned));
                        }
                        sql.delete(0, sql.length());
                        sql.append("show timeseries ")
                            .append(ExportPipelineService.formatPath(entityPath, version))
                            .append(".*");
                        SessionDataSet timeseriesSet =
                            importModel.getSession().executeQueryStatement(sql.toString());
                        columnNameList = timeseriesSet.getColumnNames();
                        Map<String, TSDataType> tsDataTypeMap = new HashMap<>();
                        while (timeseriesSet.hasNext()) {
                          RowRecord record = timeseriesSet.next();
                          int position = columnNameList.indexOf("timeseries");
                          String timeseries = record.getFields().get(position).getStringValue();
                          position = columnNameList.indexOf("dataType");
                          String type = record.getFields().get(position).getStringValue();
                          tsDataTypeMap.put(
                              timeseries, importPipelineService.parseTsDataType(type));
                        }

                        if (tsDataTypeMap.size() == 0) {
                          throw new ParamCheckException(
                              "the timeseries of device:"
                                  + ExportPipelineService.formatPath(entityPath, version)
                                  + "do not exist");
                        }

                        while (it.hasNext()) {
                          TimeSeriesRowModel timeSeriesRowModel = new TimeSeriesRowModel();
                          timeSeriesRowModel.setDeviceModel(deviceModel);
                          CSVRecord record = it.next();
                          for (String header : fileHeaderMap.keySet()) {
                            if ("Time".equals(header)) {
                              timeSeriesRowModel.setTimestamp(
                                  record.get(fileHeaderMap.get(header)));
                              continue;
                            }
                            IField iField = new IField();
                            iField.setColumnName(header);
                            Field field = new Field(tsDataTypeMap.get(header));
                            try {
                              field =
                                  importPipelineService.generateFieldValue(
                                      field, record.get(fileHeaderMap.get(header)));
                            } catch (Exception e) {
                              log.error("异常信息:", e);
                            }

                            iField.setField(FieldCopy.copy(field));
                            if (timeSeriesRowModel.getIFieldList() == null) {
                              timeSeriesRowModel.setIFieldList(new ArrayList<>());
                            }
                            timeSeriesRowModel.getIFieldList().add(iField);
                          }
                          sink.next(timeSeriesRowModel);
                        }
                        TimeSeriesRowModel finishRowModel = new TimeSeriesRowModel();
                        DeviceModel finishDeviceModel = new DeviceModel();
                        StringBuilder builder = new StringBuilder();
                        builder.append("finish,").append(deviceModel.getDeviceName());
                        finishDeviceModel.setDeviceName(builder.toString());
                        finishRowModel.setDeviceModel(finishDeviceModel);
                        finishRowModel.setIFieldList(new ArrayList<>());
                        sink.next(finishRowModel);
                      }
                      sink.complete();
                    } catch (IOException
                        | StatementExecutionException
                        | IoTDBConnectionException
                        | ParamCheckException e) {
                      sink.error(e);
                    } finally {
                      try {
                        if (in != null) {
                          in.close();
                        }
                      } catch (IOException e) {
                        log.error("异常信息:", e);
                      }
                    }
                  });
        });
  }

  public FilenameFilter initFileFilter(ImportModel importModel) {
    return (dir, name) -> {
      switch (importModel.getCompressEnum()) {
        case CSV:
          if (!name.toLowerCase().endsWith(".csv")) {
            return false;
          }
          return true;
      }
      return false;
    };
  }

  public InCsvDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public InCsvDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    this.scheduler = Schedulers.newParallel("csv-pipeline-thread", this.parallelism);
    if (this.importPipelineService == null) {
      this.importPipelineService = ImportPipelineService.importPipelineService();
    }
  }
}
