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

package org.apache.iotdb.commons.pipe.config.constant;

import org.apache.iotdb.commons.i18n.PipeMessages;

public class PipeRPCMessageConstant {
  // These two message are used in multi-modules such as pipe and IoTV2
  public static final String PIPE_ALREADY_EXIST_MSG =
      PipeMessages.MESSAGE_THE_PIPE_WITH_THE_SAME_NAME_HAS_BEEN_CREATED_6D246B19;
  public static final String PIPE_NOT_EXIST_MSG =
      PipeMessages.MESSAGE_THE_PIPE_DOES_NOT_EXIST_91EBC1F4;

  private PipeRPCMessageConstant() {
    throw new IllegalStateException(PipeMessages.UTILITY_CLASS);
  }
}
