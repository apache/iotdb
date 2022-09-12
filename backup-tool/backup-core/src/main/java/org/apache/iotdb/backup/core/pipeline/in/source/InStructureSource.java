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

import org.apache.iotdb.backup.core.model.DeviceModel;
import org.apache.iotdb.backup.core.model.FieldCopy;
import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSource;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.backup.core.service.ImportPipelineService;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;

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

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class InStructureSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(InStructureSource.class);

  private String name;

  private ImportPipelineService importPipelineService;

  private Scheduler scheduler = Schedulers.newParallel("stucture-pipeline-thread", 1);

  private CSVParser[] csvParsers = new CSVParser[1];

  private static final String TIMESERIES_STRUCTURE = "TIMESERIES_STRUCTURE.STRUCTURE";

  @Override
  public Function<Flux<String>, Flux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s ->
                    Flux.deferContextual(
                        contextView -> {
                          PipelineContext<ImportModel> context = contextView.get("pipelineContext");
                          ImportModel importModel = context.getModel();
                          return this.parseFluxFileName(initFileFilter());
                        }))
            .parallel(1)
            .runOn(scheduler)
            .flatMap(this::parseTimeSeriesRowModel)
            .transform(doNext())
            .sequential()
            .doFinally(
                signalType -> {
                  scheduler.dispose();
                });
  }

  /**
   * 获取文件名的flux filter 排除不需要的文件
   *
   * @return
   */
  public Flux<InputStream> parseFluxFileName(FilenameFilter filter) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ImportModel> context = contextView.get("pipelineContext");
          ImportModel importModel = context.getModel();
          String dic = importModel.getFileFolder();
          File[] fileArray = importPipelineService.getFileArray(filter, dic);
          List<InputStream> inputStreamList = new ArrayList<>();
          try {
            for (int i = 0; i < fileArray.length; i++) {
              File file = fileArray[i];
              InputStream in = null;
              in = new FileInputStream(file);
              inputStreamList.add(in);
            }
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          return Flux.fromIterable(inputStreamList);
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

                      while (it.hasNext()) {
                        CSVRecord record = it.next();
                        TimeSeriesRowModel rowModel = new TimeSeriesRowModel();
                        List<IField> iFieldList = new ArrayList<>();
                        DeviceModel deviceModel = new DeviceModel();
                        String alignedString =
                            parseCsvString(record.get(fileHeaderMap.get("aligned")));
                        deviceModel.setAligned(Boolean.parseBoolean(alignedString));
                        String deviceId =
                            parseCsvString(record.get(fileHeaderMap.get("timeseries")));
                        deviceModel.setDeviceName(deviceId.substring(0, deviceId.lastIndexOf(".")));
                        for (String header : fileHeaderMap.keySet()) {
                          if (!"aligned".equals(header)) {
                            IField iField = new IField();
                            Field field = new Field(TEXT);
                            field =
                                generateFieldValue(field, record.get(fileHeaderMap.get(header)));
                            iField.setField(FieldCopy.copy(field));
                            iField.setColumnName(header);
                            iFieldList.add(iField);
                          }
                        }
                        rowModel.setIFieldList(iFieldList);
                        rowModel.setDeviceModel(deviceModel);
                        sink.next(rowModel);
                      }
                      sink.complete();
                    } catch (IOException e) {
                      sink.error(e);
                    } finally {
                      try {
                        in.close();
                      } catch (IOException e) {
                        e.printStackTrace();
                      }
                    }
                  });
        });
  }

  private Field generateFieldValue(Field field, String s) {
    if (s == null || "".equals(s)) {
      return null;
    }
    switch (field.getDataType()) {
      case TEXT:
        if (s.startsWith("\"") && s.endsWith("\"")) {
          s = s.substring(1, s.length() - 1);
        }
        field.setBinaryV(Binary.valueOf(s));
        break;
      case BOOLEAN:
        field.setBoolV(Boolean.parseBoolean(s));
        break;
      case INT32:
        field.setIntV(Integer.parseInt(s));
        break;
      case INT64:
        field.setLongV(Long.parseLong(s));
        break;
      case FLOAT:
        field.setFloatV(Float.parseFloat(s));
        break;
      case DOUBLE:
        field.setDoubleV(Double.parseDouble(s));
        break;
      default:
        throw new IllegalArgumentException(": not support type,can not convert to TSDataType");
    }
    return field;
  }

  private String parseCsvString(String s) {
    if (s == null || "".equals(s)) {
      return null;
    }
    if (s.startsWith("\"") && s.endsWith("\"")) {
      s = s.substring(1, s.length() - 1);
    }
    return s;
  }

  public FilenameFilter initFileFilter() {
    return (dir, name) -> {
      if (TIMESERIES_STRUCTURE.equals(name)) {
        return true;
      }
      return false;
    };
  }

  public InStructureSource(String name) {
    this.name = name;
    if (this.importPipelineService == null) {
      this.importPipelineService = ImportPipelineService.importPipelineService();
    }
  }
}
