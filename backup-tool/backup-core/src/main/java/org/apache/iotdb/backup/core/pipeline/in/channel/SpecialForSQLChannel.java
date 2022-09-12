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

import org.apache.iotdb.backup.core.pipeline.PipeChannel;

import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

/** 转化数据，主要处理字符串 */
public class SpecialForSQLChannel
    extends PipeChannel<String, String, Function<ParallelFlux<String>, ParallelFlux<String>>> {

  private String name;

  public SpecialForSQLChannel(String name) {
    this.name = name;
  }

  @Override
  public Function<ParallelFlux<String>, ParallelFlux<String>> doExecute() {
    return flux -> flux.transform(doNext());
  }
}
