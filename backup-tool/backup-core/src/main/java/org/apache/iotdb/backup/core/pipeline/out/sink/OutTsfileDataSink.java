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
import org.apache.iotdb.backup.core.model.TimeseriesModel;
import org.apache.iotdb.backup.core.pipeline.PipeSink;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.service.ExportPipelineService;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.lang3.tuple.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.ParallelFlux;

import java.io.File;
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
                                    ConcurrentMap<String, Pair<DeviceModel, List<TimeseriesModel>>>
                                        deviceInfoMap = contextView.get("deviceInfoMap");
                                    Integer[] totalSize = contextView.get("totalSize");
                                    totalFileNum = totalSize[0];
                                    PipelineContext<ExportModel> pcontext =
                                        contextView.get("pipelineContext");
                                    int virtualSGNum =
                                        pcontext.getModel().getVirutalStorageGroupNum();
                                    long partitionInterval =
                                        pcontext.getModel().getPartitionInterval();
                                    Map<String, List<TimeSeriesRowModel>> groupByDeviceIdMap =
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
                                    for (String deviceIdKey : groupByDeviceIdMap.keySet()) {
                                      if (deviceIdKey.startsWith("finish")) {
                                        finishedFileNum.incrementAndGet();
                                        continue;
                                      }
                                      Map<String, List<TimeSeriesRowModel>>
                                          groupByTsfileNameKeyMap =
                                              groupByDeviceIdMap.get(deviceIdKey).stream()
                                                  .collect(
                                                      Collectors.toMap(
                                                          k ->
                                                              getTsfileName(
                                                                  k.getDeviceModel()
                                                                      .getDeviceName(),
                                                                  k.getTimestamp(),
                                                                  virtualSGNum,
                                                                  partitionInterval),
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
                                      for (String tsfileNameKey :
                                          groupByTsfileNameKeyMap.keySet()) {

                                        List<TimeSeriesRowModel> groupByTsfileNameKeyList =
                                            groupByTsfileNameKeyMap.get(tsfileNameKey);
                                        // 需要 根据deviceName 来判断他属于那个tsfile
                                        TsFileWriter tsFileWriter =
                                            getTsfileWriter(
                                                tsfileWriterMap,
                                                deviceInfoMap,
                                                deviceIdKey,
                                                tsfileNameKey,
                                                pcontext.getModel().getFileFolder());
                                        //TODO  有很多个tsfileWriter，如何保证每个tsfileWriter都能吧deviceId注册上
                                          registDevice(tsFileWriter, deviceInfoMap, deviceIdKey);
                                        // 需要转化grouplist数据为tsfile可以写入的数据
                                        TimeSeriesRowModel firstRow =
                                            groupByTsfileNameKeyList.get(0);
                                        DeviceModel deviceModel = firstRow.getDeviceModel();
                                        List<MeasurementSchema> measurementSchemaList =
                                            firstRow.getIFieldList().stream()
                                                .map(
                                                    iField -> {
                                                      String columnName =
                                                          iField
                                                              .getColumnName()
                                                              .substring(
                                                                  deviceModel
                                                                          .getDeviceName()
                                                                          .length()
                                                                      + 1);
                                                      TSDataType tsDataType =
                                                          iField.getTsDataType();
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

                                        groupByTsfileNameKeyList.stream()
                                            .forEach(
                                                model -> {
                                                  List<FieldCopy> fields =
                                                      model.getIFieldList().stream()
                                                          .map(iField -> iField.getField())
                                                          .collect(Collectors.toList());
                                                  int rowIndex = tablet.rowSize++;
                                                  tablet.addTimestamp(
                                                      rowIndex,
                                                      Long.parseLong(model.getTimestamp()));
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
                                                    } catch (IOException
                                                        | WriteProcessException e) {
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
                                    }
                                    return Flux.fromIterable(allList);
                                  });
                            }));
  }

  public String getTsfileName(
      String deviceName, String timestamp, int virtualSGNum, long partitionInterval) {
    StringBuilder builder = new StringBuilder();
    if (virtualSGNum <= 1) {
      builder.append(virtualSGNum);
    } else {
      int virtualPrefix = Math.abs(deviceName.hashCode() % virtualSGNum);
      builder.append(virtualPrefix);
    }
    builder.append("-");

    if (partitionInterval == 0) {
      builder.append(partitionInterval);
    } else {
      Long time = Long.parseLong(timestamp);
      long partitionPrefix = time / partitionInterval;
      builder.append(partitionPrefix);
    }
    builder.append(".tsfile");
    return builder.toString();
  }

  public TsFileWriter getTsfileWriter(
      ConcurrentMap<String, TsFileWriter> tsfileWriterMap,
      ConcurrentMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap,
      String deviceId,
      String tsfileNameKey,
      String fileFolder) {
    TsFileWriter tsFileWriter = null;
    if (!tsfileWriterMap.containsKey(tsfileNameKey)) {
      try {
        File file = new File(fileFolder + File.separator + tsfileNameKey);
        tsfileWriterMap.putIfAbsent(tsfileNameKey, new TsFileWriter(file));
        tsFileWriter = tsfileWriterMap.get(tsfileNameKey);
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      tsFileWriter = tsfileWriterMap.get(tsfileNameKey);
    }
    return tsFileWriter;
  }

  public void registDevice(
      TsFileWriter tsFileWriter,
      ConcurrentMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap,
      String deviceId) {
    Pair<DeviceModel, List<TimeseriesModel>> pair;
    if (deviceInfoMap.containsKey(deviceId)) {
      synchronized (tsFileWriter){
          if(!deviceInfoMap.containsKey(deviceId)){
              return;
          }
          pair = deviceInfoMap.get(deviceId);
          List<TimeseriesModel> timeseriesModelList = pair.getRight();
          List<MeasurementSchema> measurementSchemaList =
                  timeseriesModelList.stream()
                          .map(
                                  model -> {
                                      String columnName =
                                              model.getName().substring(pair.getLeft().getDeviceName().length() + 1);
                                      TSDataType type = model.getType();
                                      MeasurementSchema measurementSchema = new MeasurementSchema(columnName, type);
                                      return measurementSchema;
                                  })
                          .collect(Collectors.toList());

          Path path = new Path(pair.getLeft().getDeviceName());
          syncRegisterTimeseries(tsFileWriter, path, measurementSchemaList, pair.getLeft().isAligned());
          deviceInfoMap.remove(deviceId);
      }
    } else {
      return;
    }
  }

  /**
   * tsfileWriter.registerTimeseries非线程安全方法，修改为线程安全的
   *
   * @param tsFileWriter
   * @param path
   * @param measurementSchemaList
   * @param isAligned
   */
  public void syncRegisterTimeseries(
      TsFileWriter tsFileWriter,
      Path path,
      List<MeasurementSchema> measurementSchemaList,
      boolean isAligned) {
    synchronized (tsFileWriter) {
      if (isAligned) {
        try {
          tsFileWriter.registerAlignedTimeseries(path, measurementSchemaList);
        } catch (WriteProcessException e) {
          e.printStackTrace();
        }
      } else {
        tsFileWriter.registerTimeseries(path, measurementSchemaList);
      }
    }
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
