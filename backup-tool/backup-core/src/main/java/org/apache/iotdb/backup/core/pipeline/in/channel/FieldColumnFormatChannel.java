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
package org.apache.iotdb.backup.core.pipeline.in.channel;

import org.apache.iotdb.backup.core.model.IField;
import org.apache.iotdb.backup.core.model.TimeSeriesRowModel;
import org.apache.iotdb.backup.core.pipeline.PipeChannel;
import org.apache.iotdb.backup.core.pipeline.context.PipelineContext;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

import java.util.List;
import java.util.function.Function;

/** */
public class FieldColumnFormatChannel
    extends PipeChannel<
        TimeSeriesRowModel,
        TimeSeriesRowModel,
        Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>>> {

  private String name;

  public FieldColumnFormatChannel(String name) {
    this.name = name;
  }

  /**
   * 把 root.test.test.cli.ali.dd 格式的timeseries，转化为 dd，即不带前缀的格式
   *
   * @return
   */
  @Override
  public Function<ParallelFlux<TimeSeriesRowModel>, ParallelFlux<TimeSeriesRowModel>> doExecute() {
    return flux ->
        flux.flatMap(
                s -> {
                  return Flux.deferContextual(
                      contextView -> {
                        PipelineContext<ImportModel> pcontext = contextView.get("pipelineContext");
                        ImportModel importModel = pcontext.getModel();
                        List<IField> list = s.getIFieldList();
                        list.stream()
                            .forEach(
                                ifield -> {
                                  if (ifield.getField() != null) {
                                    StringBuilder builder = new StringBuilder();
                                    builder.append(s.getDeviceModel().getDeviceName()).append(".");
                                    ifield.setColumnName(
                                        ifield.getColumnName().replace(builder.toString(), ""));
                                  }
                                });
                        return ParallelFlux.from(Flux.just(s));
                      });
                })
            .transform(doNext());
  }
}
