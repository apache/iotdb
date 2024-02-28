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

package org.apache.iotdb.db.pipe.processor.downsampling;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY;

public abstract class DownSamplingAbstractProcessor implements PipeProcessor {
  protected String dataBaseNameWithPathSeparator;
  protected boolean shouldSplitFile;

  protected PartialPathLastObjectCache pathLastObjectCache;

  @Override
  public void customize(
      PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration) {
    final String dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(
                new DataRegionId(
                    ((PipeTaskProcessorRuntimeEnvironment) configuration.getRuntimeEnvironment())
                        .getRegionId()))
            .getDatabaseName();

    final long memoryLimitInBytes =
        parameters.getLongOrDefault(
            PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_KEY,
            PROCESSOR_DOWN_SAMPLING_MEMORY_LIMIT_IN_BYTES_DEFAULT_VALUE);
    shouldSplitFile =
        parameters.getBooleanOrDefault(
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_KEY,
            PROCESSOR_DOWN_SAMPLING_SPLIT_FILE_DEFAULT_VALUE);

    dataBaseNameWithPathSeparator = dataBaseName + TsFileConstant.PATH_SEPARATOR;

    pathLastObjectCache = new PartialPathLastObjectCache(memoryLimitInBytes);
  }

  @Override
  public void close() throws Exception {
    if (pathLastObjectCache != null) {
      pathLastObjectCache.close();
    }
  }
}
