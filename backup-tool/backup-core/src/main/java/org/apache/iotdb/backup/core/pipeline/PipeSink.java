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
 */ package org.apache.iotdb.backup.core.pipeline;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.ParallelFlux;

import java.util.function.Function;

@Slf4j
public abstract class PipeSink<T, R>
    implements Component<Function<ParallelFlux<T>, ParallelFlux<R>>> {

  @Override
  public Function<ParallelFlux<T>, ParallelFlux<R>> execute() {
    return doExecute()
        .andThen(
            f ->
                f.doOnError(
                    e -> {
                      // log.error("异常信息:",e);
                    }));
  }

  public abstract Function<ParallelFlux<T>, ParallelFlux<R>> doExecute();

  public abstract Double[] rateOfProcess();

  public abstract Long finishedRowNum();
}
