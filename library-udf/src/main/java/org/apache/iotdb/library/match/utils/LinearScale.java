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

package org.apache.iotdb.library.match.utils;

import org.apache.iotdb.library.i18n.LibraryUdfMessages;

public class LinearScale<T extends Number> {
  private final double domainStart;
  private final double domainEnd;
  private final double rangeStart;
  private final double rangeEnd;

  public LinearScale(T domainStart, T domainEnd, double rangeStart, double rangeEnd) {
    this.domainStart = domainStart.doubleValue();
    this.domainEnd = domainEnd.doubleValue();

    if (this.domainStart >= this.domainEnd) {
      throw new IllegalArgumentException(
          LibraryUdfMessages.DOMAIN_START_MUST_BE_LESS_THAN_DOMAIN_END);
    }
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
  }

  public double scale(T value) {
    double val = value.doubleValue();
    if (val < domainStart || val > domainEnd) {
      throw new IllegalArgumentException(LibraryUdfMessages.VALUE_OUT_OF_DOMAIN_RANGE);
    }
    return rangeStart + (rangeEnd - rangeStart) * (val - domainStart) / (domainEnd - domainStart);
  }
}
