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
package org.apache.iotdb.backup.core.service;

import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.*;

/** @Author: LL @Description: @Date: create in 2022/7/1 9:41 */
public class ImportPipelineService {

  private static final Logger log = LoggerFactory.getLogger(ImportPipelineService.class);

  private static ImportPipelineService importPipelineService;

  private ImportPipelineService() {}

  public static ImportPipelineService importPipelineService() {
    if (importPipelineService == null) {
      importPipelineService = new ImportPipelineService();
    }
    return importPipelineService;
  }

  /**
   * 获取文件名的flux filter 排除不需要的文件
   *
   * @return
   */
  public Flux<InputStream> parseFluxFileName(
      FilenameFilter filter, ConcurrentHashMap<String, List<InputStream>> COMPRESS_MAP) {
    return Flux.deferContextual(
        contextView -> {
          PipelineContext<ImportModel> context = contextView.get("pipelineContext");
          ImportModel importModel = context.getModel();
          String dic = importModel.getFileFolder();
          File[] fileArray = this.getFileArray(filter, dic);
          List<InputStream> inputStreamList = new ArrayList<>();
          try {
            for (int i = 0; i < fileArray.length; i++) {
              File file = fileArray[i];
              InputStream in = new FileInputStream(file);
              inputStreamList.add(in);
            }
          } catch (IOException e) {
            log.error("异常信息:", e);
          }
          COMPRESS_MAP.put("inputStreamList", inputStreamList);
          return Flux.fromIterable(inputStreamList);
        });
  }

  public File[] getFileArray(FilenameFilter filter, String dic) {
    File fileDic = new File(dic);
    if (!fileDic.exists()) {
      throw new IllegalArgumentException("file folder does not exists");
    }
    if (!fileDic.isDirectory()) {
      throw new IllegalArgumentException("file folder is not a dictionary");
    }

    File[] fileArray = fileDic.listFiles(filter);
    return fileArray;
  }

  public Field generateFieldValue(Field field, String s) {
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

  public TSDataType parseTsDataType(String type) {
    switch (type) {
      case "TEXT":
        return TEXT;
      case "BOOLEAN":
        return BOOLEAN;
      case "INT32":
        return INT32;
      case "INT64":
        return INT64;
      case "FLOAT":
        return FLOAT;
      case "DOUBLE":
        return DOUBLE;
      default:
        throw new IllegalArgumentException(
            type + ": not support type,can not convert to TSDataType");
    }
  }
}
