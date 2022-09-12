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
package org.apache.iotdb.backup.core.pipeline.out.channel;

import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeChannel;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

/** 转化数据，主要处理字符串 */
public class StringFormatWithoutNullFieldChannel
    extends PipeChannel<
        TimeSeriesRowModel,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private String name;

  public StringFormatWithoutNullFieldChannel(String name) {
    this.name = name;
  }

  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s -> {
                  s.getIFieldList().forEach(this::sqlFileTransformer);
                  return ParallelFlux.from(Flux.just(s));
                })
            .transform(doNext());
  }

  private void sqlFileTransformer(IField iField) {
    if (iField.getField() != null
        && iField.getField().getObjectValue(iField.getField().getDataType()) != null) {
      if (iField.getField().getDataType() == TSDataType.TEXT) {
        String value = iField.getField().getStringValue();
        if (value.indexOf('\"') >= 0) {
          value = value.replaceAll("\"", "\\\"");
        }
        StringBuilder builder = new StringBuilder();
        builder.append("\"").append(value).append("\"");
        iField.getField().setBinaryV(Binary.valueOf(builder.toString()));
      }
    } else {
      iField.setField(null);
    }
  }
}
