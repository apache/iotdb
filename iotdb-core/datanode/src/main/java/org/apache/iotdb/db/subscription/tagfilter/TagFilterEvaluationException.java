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

package org.apache.iotdb.db.subscription.tagfilter;

import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

/** Raised when a TAG filter cannot be evaluated safely for an input event. */
public class TagFilterEvaluationException extends RuntimeException {

  public TagFilterEvaluationException(final String reason) {
    super(
        String.format(
            DataNodeMiscMessages.EXCEPTION_TAG_FILTER_EVALUATION_FAILED_ARG_1B239B2F, reason));
  }

  public TagFilterEvaluationException(final String reason, final Throwable cause) {
    super(
        String.format(
            DataNodeMiscMessages.EXCEPTION_TAG_FILTER_EVALUATION_FAILED_ARG_1B239B2F, reason),
        cause);
  }

  public static TagFilterEvaluationException schemaBinding(final String reason) {
    return new TagFilterEvaluationException(
        String.format(
            DataNodeMiscMessages.EXCEPTION_TAG_FILTER_SCHEMA_BINDING_FAILED_ARG_7A5D2D47, reason));
  }
}
