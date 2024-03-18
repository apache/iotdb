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

package org.apache.iotdb.db.pipe.processor.aggregate.windowing;

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_BOUNDARY_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SLIDING_SECONDS_KEY;

public class TumblingWindowingProcessor extends AbstractWindowingProcessor {
  private long slidingBoundaryTime;

  private long slidingInterval;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (long) args > 0,
        String.format("The parameter %s must be greater than 0", PROCESSOR_SLIDING_SECONDS_KEY),
        parameters.getLongOrDefault(
            PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    slidingBoundaryTime =
        parameters.hasAnyAttributes(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY)
            ? DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                parameters.getString(PROCESSOR_SLIDING_BOUNDARY_TIME_KEY))
            : PROCESSOR_SLIDING_BOUNDARY_TIME_DEFAULT_VALUE;
    slidingInterval =
        TimestampPrecisionUtils.convertToCurrPrecision(
            parameters.getLongOrDefault(
                PROCESSOR_SLIDING_SECONDS_KEY, PROCESSOR_SLIDING_SECONDS_DEFAULT_VALUE),
            TimeUnit.SECONDS);
  }
}
