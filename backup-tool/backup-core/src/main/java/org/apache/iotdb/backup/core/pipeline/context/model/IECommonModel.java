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
package org.apache.iotdb.backup.core.pipeline.context.model;

import org.apache.iotdb.session.Session;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.Data;
import reactor.core.publisher.SignalType;

import java.util.function.Consumer;

@Data
public class IECommonModel extends PipelineModel {

  @JSONField(serialize = false)
  private Session session;

  @JSONField(serialzeFeatures = {SerializerFeature.WriteEnumUsingToString})
  private CompressEnum compressEnum;

  private String charSet;

  private String fileFolder;

  @JSONField(serialzeFeatures = {SerializerFeature.WriteEnumUsingToString})
  private FileSinkStrategyEnum fileSinkStrategyEnum;

  private Boolean needTimeseriesStructure;

  @JSONField(serialize = false)
  @Deprecated
  private Boolean zipCompress;

  @JSONField(serialize = false)
  private int parallelism;

  // 回调方法，pipeline运行完毕后悔调用此方法
  @JSONField(serialize = false)
  private Consumer<SignalType> consumer;

  // 回调方法，pipeline出现异常调用此方法
  @JSONField(serialize = false)
  private Consumer<Throwable> e;
}
