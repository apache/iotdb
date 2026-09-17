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

package org.apache.iotdb.commons.lbac;

import org.apache.iotdb.commons.i18n.LBACMessages;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** The object-side LBAC requirement: protected column labels grouped by READ and WRITE. */
public final class RequiredLabels {

  private final Set<String> readLabels = new HashSet<>();
  private final Set<String> writeLabels = new HashSet<>();

  public RequiredLabels requireLabelWithReadAccess(final String labelName) {
    Objects.requireNonNull(labelName, LBACMessages.EXCEPTION_LABELNAME_IS_NULL_856ABAE4);
    readLabels.add(labelName);
    return this;
  }

  public RequiredLabels requireLabelWithWriteAccess(final String labelName) {
    Objects.requireNonNull(labelName, LBACMessages.EXCEPTION_LABELNAME_IS_NULL_856ABAE4);
    writeLabels.add(labelName);
    return this;
  }

  public RequiredLabels requireLabelWithAllAccess(final String labelName) {
    Objects.requireNonNull(labelName, LBACMessages.EXCEPTION_LABELNAME_IS_NULL_856ABAE4);
    readLabels.add(labelName);
    writeLabels.add(labelName);
    return this;
  }

  public Set<String> getReadLabels() {
    return readLabels;
  }

  public Set<String> getWriteLabels() {
    return writeLabels;
  }

  public boolean isEmpty() {
    return readLabels.isEmpty() && writeLabels.isEmpty();
  }
}
