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
import org.apache.iotdb.backup.core.utils.ToByteArrayUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class InCompressDataSource
    extends PipeSource<
        String,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private static final Logger log = LoggerFactory.getLogger(InCompressDataSource.class);

  private String name;

  private ImportPipelineService importPipelineService;

  private Scheduler scheduler;

  private static final String CATALOG_COMPRESS = "CATALOG_COMPRESS.CATALOG";

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
                      Pair<String[], String> pair = parseCompressHeader(in);
                      String[] timeseries = pair.getLeft();
                      String deviceName = null;
                      if (timeseries.length != 0) {
                        deviceName = timeseries[0].substring(0, timeseries[0].lastIndexOf("."));
                      }

                      StringBuilder sql = new StringBuilder();
                      sql.append(" show devices ")
                          .append(ExportPipelineService.formatPath(deviceName, version));
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
                        deviceModel.setDeviceName(deviceName);
                        deviceModel.setAligned(Boolean.parseBoolean(aligned));
                      }
                      sql.delete(0, sql.length());
                      sql.append("show timeseries ")
                          .append(ExportPipelineService.formatPath(deviceName, version))
                          .append(".*");
                      SessionDataSet timeseriesSet =
                          importModel.getSession().executeQueryStatement(sql.toString());
                      columnNameList = timeseriesSet.getColumnNames();
                      Map<String, TSDataType> tsDataTypeMap = new HashMap<>();
                      while (timeseriesSet.hasNext()) {
                        RowRecord record = timeseriesSet.next();
                        int position = columnNameList.indexOf("timeseries");
                        String measurement = record.getFields().get(position).getStringValue();
                        position = columnNameList.indexOf("dataType");
                        String type = record.getFields().get(position).getStringValue();
                        tsDataTypeMap.put(measurement, importPipelineService.parseTsDataType(type));
                      }
                      if (tsDataTypeMap.size() == 0) {
                        throw new ParamCheckException(
                            "the timeseries of device:"
                                + ExportPipelineService.formatPath(deviceName, version)
                                + " does not exist");
                      }

                      while (true) {
                        List<TimeSeriesRowModel> rowModelList = new ArrayList<>();
                        List<String[]> data =
                            parseCompressData(in, pair.getLeft().length + 1, pair.getRight());
                        if (data.size() == 0) {
                          break;
                        }
                        for (String times : data.get(0)) {
                          TimeSeriesRowModel rowModel = new TimeSeriesRowModel();
                          rowModel.setTimestamp(times);
                          rowModel.setDeviceModel(deviceModel);
                          rowModelList.add(rowModel);
                        }

                        for (int i = 1; i < data.size(); i++) {
                          String[] v = data.get(i);
                          for (int j = 0; j < v.length; j++) {
                            TimeSeriesRowModel rowModel = rowModelList.get(j);
                            if (rowModel.getIFieldList() == null) {
                              rowModel.setIFieldList(new ArrayList<>());
                            }
                            String value = v[j];
                            String measurement = timeseries[i - 1];
                            TSDataType type = tsDataTypeMap.get(timeseries[i - 1]);
                            IField iField = new IField();
                            iField.setColumnName(measurement);
                            if (value == null) {
                              iField.setField(null);
                            } else {
                              Field field = new Field(type);
                              field = importPipelineService.generateFieldValue(field, value);
                              iField.setField(FieldCopy.copy(field));
                            }
                            rowModel.getIFieldList().add(iField);
                          }
                        }
                        for (TimeSeriesRowModel rowModel : rowModelList) {
                          sink.next(rowModel);
                        }
                      }
                      TimeSeriesRowModel finishRowModel = new TimeSeriesRowModel();
                      DeviceModel finishDeviceModel = new DeviceModel();
                      StringBuilder builder = new StringBuilder();
                      builder.append("finish,").append(deviceName);
                      finishDeviceModel.setDeviceName(builder.toString());
                      finishRowModel.setDeviceModel(finishDeviceModel);
                      finishRowModel.setIFieldList(new ArrayList<>());
                      sink.next(finishRowModel);
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
        case SNAPPY:
          if (!name.toLowerCase().endsWith(".snappy.bin")) {
            return false;
          }
          return true;
        case GZIP:
          if (!name.toLowerCase().endsWith(".gz.bin")) {
            return false;
          }
          return true;
        case LZ4:
          if (!name.toLowerCase().endsWith(".lz4.bin")) {
            return false;
          }
          return true;
        default:
          if (!name.toLowerCase().endsWith(".bin")) {
            return false;
          }
          return true;
      }
    };
  }

  // 解析header， 获取设备对应的measurement属性
  public Pair<String[], String> parseCompressHeader(InputStream in) throws IOException {
    byte[] b = new byte[12];
    in.read(b, 0, 12);

    String mo = new String(ArrayUtils.subarray(b, 0, 4));
    short version = ToByteArrayUtils.byteArrayToShort(ArrayUtils.subarray(b, 4, 6));
    String compressType = new String(ArrayUtils.subarray(b, 6, 8));
    int length = ToByteArrayUtils.byteArrayToInt(ArrayUtils.subarray(b, 8, 12));

    byte[] tb = new byte[length];
    in.read(tb, 0, length);
    String[] header = (String[]) ToByteArrayUtils.convertToObject(tb);
    return Pair.of(header, compressType);
  }

  public List<String[]> parseCompressData(InputStream in, int lopSize, String compressType)
      throws IOException {
    byte[] block = new byte[5];
    int flag = in.read(block, 0, 5);
    List<String[]> data = new ArrayList<>();
    if (flag != -1) {
      for (int i = 0; i < lopSize; i++) {
        byte[] b = new byte[8];
        in.read(b, 0, 8);
        int originalLength = ToByteArrayUtils.byteArrayToInt(ArrayUtils.subarray(b, 0, 4));
        int compressedLength = ToByteArrayUtils.byteArrayToInt(ArrayUtils.subarray(b, 4, 8));
        byte[] compressedValue = new byte[compressedLength];
        in.read(compressedValue, 0, compressedLength);
        byte[] originalBytes;
        switch (compressType) {
          case "01":
            originalBytes = Snappy.uncompress(compressedValue);
            break;
          case "02":
            originalBytes = ICompressor.GZIPCompress.uncompress(compressedValue);
            break;
          case "03":
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            originalBytes = new byte[originalLength];
            decompressor.decompress(compressedValue, originalBytes);
            break;
          default:
            throw new IllegalStateException("Unexpected value: " + compressType);
        }
        String[] originalArr = (String[]) ToByteArrayUtils.convertToObject(originalBytes);
        data.add(originalArr);
      }
    }
    return data;
  }

  public InCompressDataSource(String name) {
    this(name, Schedulers.DEFAULT_POOL_SIZE);
  }

  public InCompressDataSource(String name, int parallelism) {
    this.name = name;
    this.parallelism = parallelism <= 0 ? Schedulers.DEFAULT_POOL_SIZE : parallelism;
    this.scheduler = Schedulers.newParallel("compress-pipeline-thread", this.parallelism);
    if (this.importPipelineService == null) {
      this.importPipelineService = ImportPipelineService.importPipelineService();
    }
  }
}
