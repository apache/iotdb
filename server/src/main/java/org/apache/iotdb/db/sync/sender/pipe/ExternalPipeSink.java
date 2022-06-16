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
 *
 */
package org.apache.iotdb.db.sync.sender.pipe;

import org.apache.iotdb.db.exception.sync.PipeSinkException;
import org.apache.iotdb.db.sync.externalpipe.ExtPipePluginRegister;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExternalPipeSink implements PipeSink {
  private static final Logger logger = LoggerFactory.getLogger(ExternalPipeSink.class);

  private final PipeSinkType pipeSinkType = PipeSinkType.ExternalPipe;

  private final String pipeSinkName;
  private final String extPipeSinkTypeName;

  private Map<String, String> sinkParams;

  public ExternalPipeSink(String pipeSinkName, String extPipeSinkTypeName) {
    this.pipeSinkName = pipeSinkName;
    this.extPipeSinkTypeName = extPipeSinkTypeName;
  }

  @Override
  public void setAttribute(List<Pair<String, String>> params) throws PipeSinkException {
    String regex = "^'|'$|^\"|\"$";
    sinkParams =
        params.stream()
            .collect(
                Collectors.toMap(
                    e -> e.left, e -> e.right.trim().replaceAll(regex, ""), (key1, key2) -> key2));

    try {
      ExtPipePluginRegister.getInstance()
          .getWriteFactory(extPipeSinkTypeName)
          .validateSinkParams(sinkParams);
    } catch (Exception e) {
      throw new PipeSinkException(e.getMessage());
    }
  }

  @Override
  public String getPipeSinkName() {
    return pipeSinkName;
  }

  @Override
  public PipeSinkType getType() {
    return pipeSinkType;
  }

  @Override
  public String showAllAttributes() {
    // HACK: should provide an interface 'getDisplayableAttributes(Map<String, String>)' and let the
    // plugin decide which attribute is suitable for being displayed.
    return sinkParams.entrySet().stream()
        .filter(e -> !e.getKey().contains("access_key"))
        .collect(Collectors.toList())
        .toString();
  }

  public Map<String, String> getSinkParams() {
    return sinkParams;
  }

  public String getExtPipeSinkTypeName() {
    return extPipeSinkTypeName;
  }
}
