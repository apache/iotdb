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

import org.apache.iotdb.backup.core.model.DeviceModel;
import org.apache.iotdb.backup.core.model.FieldCopy;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/** @Author: LL @Description: @Date: create in 2022/11/24 17:11 */
public class OutTsfileDataSink extends PipeSink<TimeSeriesRowModel, TimeSeriesRowModel> {

  private static final Logger log = LoggerFactory.getLogger(OutTsfileDataSink.class);

  private String name;

  private AtomicInteger finishedFileNum = new AtomicInteger();

  private int totalFileNum;

  private AtomicLong finishedRowNum = new AtomicLong();

  private ExportPipelineService exportPipelineService;

  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return sink ->
        sink.transformGroups(
            (Function<
                    GroupedFlux<Integer, TimeSeriesRowModel>,
                    Publisher<? extends TimeSeriesRowModel>>)
                integerTimeSeriesRowModelGroupedFlux ->
                    integerTimeSeriesRowModelGroupedFlux
                        .buffer(15000, 15000)
                        .flatMap(
                            allList -> {
                              return Flux.deferContextual(
                                  contextView -> {
                                    ConcurrentMap<String, TsFileWriter> tsfileWriterMap =
                                        contextView.get("tsfileWriterMap");
                                    Integer[] totalSize = contextView.get("totalSize");
                                    totalFileNum = totalSize[0];
                                    PipelineContext<ExportModel> pcontext =
                                        contextView.get("pipelineContext");
                                    Map<String, List<TimeSeriesRowModel>> groupMap =
                                        allList.stream()
                                            .collect(
                                                Collectors.toMap(
                                                    k -> k.getDeviceModel().getDeviceName(),
                                                    p -> {
                                                      List<TimeSeriesRowModel> result =
                                                          new ArrayList();
                                                      result.add(p);
                                                      return result;
                                                    },
                                                    (o, n) -> {
                                                      o.addAll(n);
                                                      return o;
                                                    }));
                                    for (String groupKey : groupMap.keySet()) {
                                      if (groupKey.startsWith("finish")) {
                                        finishedFileNum.incrementAndGet();
                                        continue;
                                      }
                                      List<TimeSeriesRowModel> groupList = groupMap.get(groupKey);

                                      // 需要 根据deviceName 来判断他属于那个tsfile
                                      TsFileWriter tsFileWriter = tsfileWriterMap.get("tsfile-0");
                                      // 需要转化grouplist数据为tsfile可以写入的数据
                                      TimeSeriesRowModel firstRow = groupList.get(0);
                                      DeviceModel deviceModel = firstRow.getDeviceModel();
                                      List<MeasurementSchema> measurementSchemaList =
                                          firstRow.getIFieldList().stream()
                                              .map(
                                                  iField -> {
                                                    String columnName =
                                                        iField
                                                            .getColumnName()
                                                            .substring(
                                                                deviceModel.getDeviceName().length()
                                                                    + 1);
                                                    TSDataType tsDataType = iField.getTsDataType();
                                                    MeasurementSchema measurementSchema =
                                                        new MeasurementSchema(
                                                            columnName, tsDataType);
                                                    return measurementSchema;
                                                  })
                                              .collect(Collectors.toList());

                                      Tablet tablet =
                                          new Tablet(
                                              deviceModel.getDeviceName(), measurementSchemaList);
                                      tablet.initBitMaps();

                                      groupList.stream()
                                          .forEach(
                                              model -> {
                                                List<FieldCopy> fields =
                                                    model.getIFieldList().stream()
                                                        .map(iField -> iField.getField())
                                                        .collect(Collectors.toList());
                                                int rowIndex = tablet.rowSize++;
                                                tablet.addTimestamp(
                                                    rowIndex, Long.parseLong(model.getTimestamp()));
                                                for (int i = 0; i < fields.size(); ) {
                                                  List<MeasurementSchema> schemas =
                                                      tablet.getSchemas();
                                                  for (int j = 0; j < schemas.size(); j++) {
                                                    MeasurementSchema measurementSchema =
                                                        schemas.get(j);
                                                    Object value =
                                                        fields
                                                            .get(i)
                                                            .getObjectValue(
                                                                measurementSchema.getType());
                                                    if (value == null) {
                                                      tablet.bitMaps[j].mark(rowIndex);
                                                    }
                                                    tablet.addValue(
                                                        measurementSchema.getMeasurementId(),
                                                        rowIndex,
                                                        value);
                                                    i++;
                                                  }
                                                }

                                                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                                                  try {
                                                    exportPipelineService.syncWriteByTsfileWriter(
                                                        tsFileWriter,
                                                        tablet,
                                                        deviceModel.isAligned());
                                                    finishedRowNum.addAndGet(tablet.rowSize);
                                                    tablet.initBitMaps();
                                                    tablet.reset();
                                                  } catch (IOException | WriteProcessException e) {
                                                    e.printStackTrace();
                                                  }
                                                }
                                              });

                                      try {
                                        if (tablet.rowSize != 0) {
                                          exportPipelineService.syncWriteByTsfileWriter(
                                              tsFileWriter, tablet, deviceModel.isAligned());
                                          finishedRowNum.addAndGet(tablet.rowSize);
                                        }
                                      } catch (IOException | WriteProcessException e) {
                                        e.printStackTrace();
                                      }
                                    }
                                    return Flux.fromIterable(allList);
                                  });
                            }));
  }

  @Override
  public Double[] rateOfProcess() {
    log.info("已经导出文件：{}", finishedFileNum);
    log.info("总文件数：{}", totalFileNum);
    Double[] rateDouble = new Double[2];
    rateDouble[0] = finishedFileNum.doubleValue();
    rateDouble[1] = Double.parseDouble(String.valueOf(totalFileNum));
    return rateDouble;
  }

  @Override
  public Long finishedRowNum() {
    return finishedRowNum.get();
  }

  public OutTsfileDataSink(String name) {
    this.name = name;
    this.exportPipelineService = ExportPipelineService.exportPipelineService();
  }
}
