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
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
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
  // 数据条数
  private AtomicLong finishedRowNum = new AtomicLong();

  private ExportPipelineService exportPipelineService;

  List<TimeSeriesRowModel> allList = new ArrayList();

  ConcurrentMap<String, TsFileWriter> tsfileWriterMap;
  ConcurrentMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap;
  ConcurrentMap<String, Schema> schemaMap;
  Integer[] totalSize;
  PipelineContext<ExportModel> pcontext;

  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return sink ->
        sink.flatMap(
                s -> {
                  synchronized (allList) {
                    allList.add(s);
                  }
                  return Flux.just(s);
                })
            .flatMap(
                s -> {
                  return Flux.deferContextual(
                      contextView -> {
                        tsfileWriterMap = contextView.get("tsfileWriterMap");
                        deviceInfoMap = contextView.get("deviceInfoMap");
                        schemaMap = contextView.get("schemaMap");
                        totalSize = contextView.get("totalSize");
                        pcontext = contextView.get("pipelineContext");
                        synchronized (allList) {
                          if (allList.size() >= 100000) {
                            doSink(allList);
                          }
                        }
                        return Flux.just(s);
                      });
                })
            .sequential()
            .doOnComplete(
                () -> {
                  synchronized (allList) {
                    doSink(allList);
                  }
                })
            .parallel();
  }

  private void doSink(List<TimeSeriesRowModel> bufferList) {
    totalFileNum = totalSize[0];
    int virtualSGNum = pcontext.getModel().getVirutalStorageGroupNum();
    long partitionInterval = pcontext.getModel().getPartitionInterval();

    Map<String, List<TimeSeriesRowModel>> groupByDeviceIdMap =
        bufferList.stream()
            .collect(
                Collectors.toMap(
                    k -> k.getDeviceModel().getDeviceName(),
                    p -> {
                      List<TimeSeriesRowModel> result = new ArrayList();
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
      Map<String, List<TimeSeriesRowModel>> groupByTsfileNameKeyMap =
          groupByDeviceIdMap.get(deviceIdKey).stream()
              .collect(
                  Collectors.toMap(
                      k ->
                          getTsfileName(
                              k.getDeviceModel().getDeviceName(),
                              k.getTimestamp(),
                              virtualSGNum,
                              partitionInterval),
                      p -> {
                        List<TimeSeriesRowModel> result = new ArrayList();
                        result.add(p);
                        return result;
                      },
                      (o, n) -> {
                        o.addAll(n);
                        return o;
                      }));
      for (String tsfileNameKey : groupByTsfileNameKeyMap.keySet()) {

        List<TimeSeriesRowModel> groupByTsfileNameKeyList =
            groupByTsfileNameKeyMap.get(tsfileNameKey);
        // 需要 根据deviceName 来判断他属于那个tsfile
        TsFileWriter tsFileWriter =
            getTsfileWriter(
                tsfileWriterMap,
                schemaMap,
                deviceIdKey,
                tsfileNameKey,
                pcontext.getModel().getFileFolder());
        Schema schema = schemaMap.get(tsfileNameKey);
        registDevice(tsFileWriter, schema, deviceInfoMap, deviceIdKey);
        // 需要转化grouplist数据为tsfile可以写入的数据
        TimeSeriesRowModel firstRow = groupByTsfileNameKeyList.get(0);
        DeviceModel deviceModel = firstRow.getDeviceModel();
        List<MeasurementSchema> measurementSchemaList =
            firstRow.getIFieldList().stream()
                .map(
                    iField -> {
                      String columnName =
                          iField
                              .getColumnName()
                              .substring(deviceModel.getDeviceName().length() + 1);
                      TSDataType tsDataType = iField.getTsDataType();
                      MeasurementSchema measurementSchema =
                          new MeasurementSchema(columnName, tsDataType);
                      return measurementSchema;
                    })
                .collect(Collectors.toList());

        Tablet tablet = new Tablet(deviceModel.getDeviceName(), measurementSchemaList);
        tablet.initBitMaps();

        groupByTsfileNameKeyList.stream()
            .forEach(
                model -> {
                  List<FieldCopy> fields =
                      model.getIFieldList().stream()
                          .map(iField -> iField.getField())
                          .collect(Collectors.toList());
                  int rowIndex = tablet.rowSize++;
                  tablet.addTimestamp(rowIndex, Long.parseLong(model.getTimestamp()));
                  for (int i = 0; i < fields.size(); ) {
                    List<MeasurementSchema> schemas = tablet.getSchemas();
                    for (int j = 0; j < schemas.size(); j++) {
                      MeasurementSchema measurementSchema = schemas.get(j);
                      Object value = fields.get(i).getObjectValue(measurementSchema.getType());
                      if (value == null) {
                        tablet.bitMaps[j].mark(rowIndex);
                      }
                      tablet.addValue(measurementSchema.getMeasurementId(), rowIndex, value);
                      i++;
                    }
                  }

                  if (tablet.rowSize == tablet.getMaxRowNumber()) {
                    try {
                      exportPipelineService.syncWriteByTsfileWriter(
                          tsFileWriter, tablet, deviceModel.isAligned());
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
          tsFileWriter.flushAllChunkGroups();
        } catch (IOException | WriteProcessException e) {
          e.printStackTrace();
        }
      }
    }
    bufferList.clear();
  }

  private String getTsfileName(
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
      long partitionPrefix = time / (partitionInterval * 1000L);
      builder.append(partitionPrefix);
    }
    builder.append(".tsfile");
    return builder.toString();
  }

  private TsFileWriter getTsfileWriter(
      ConcurrentMap<String, TsFileWriter> tsfileWriterMap,
      ConcurrentMap<String, Schema> schemaMap,
      String deviceId,
      String tsfileNameKey,
      String fileFolder) {
    TsFileWriter tsFileWriter = null;
    synchronized (tsfileWriterMap) {
      if (!tsfileWriterMap.containsKey(tsfileNameKey)) {
        try {
          File file = new File(fileFolder + File.separator + tsfileNameKey);
          Schema schema = new Schema();
          //          TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
          //          config.setGroupSizeInByte(16 * 1024 * 1024);
          //          tsfileWriterMap.putIfAbsent(tsfileNameKey, new TsFileWriter(file, schema,
          // config));
          tsfileWriterMap.putIfAbsent(tsfileNameKey, new TsFileWriter(file, schema));
          schemaMap.putIfAbsent(tsfileNameKey, schema);
          tsFileWriter = tsfileWriterMap.get(tsfileNameKey);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        tsFileWriter = tsfileWriterMap.get(tsfileNameKey);
      }
      return tsFileWriter;
    }
  }

  private void registDevice(
      TsFileWriter tsFileWriter,
      Schema schema,
      ConcurrentMap<String, Pair<DeviceModel, List<TimeseriesModel>>> deviceInfoMap,
      String deviceId) {
    boolean needRegist = true;
    Pair<DeviceModel, List<TimeseriesModel>> pair = deviceInfoMap.get(deviceId);
    Path path = new Path(pair.getLeft().getDeviceName());
    synchronized (schema) {
      if (schema.getRegisteredTimeseriesMap().size() == 0) {
        needRegist = true;
      } else if (!schema.containsDevice(path)) {
        needRegist = true;
      } else {
        needRegist = false;
      }
    }

    if (needRegist) {
      synchronized (tsFileWriter) {
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

        syncRegisterTimeseries(
            tsFileWriter, path, measurementSchemaList, pair.getLeft().isAligned());
      }
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
  private void syncRegisterTimeseries(
      TsFileWriter tsFileWriter,
      Path path,
      List<MeasurementSchema> measurementSchemaList,
      boolean isAligned) {
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
