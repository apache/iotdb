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

public class LinearScale<T extends Number> {
  private final double domainStart;
  private final double domainEnd;
  private final double rangeStart;
  private final double rangeEnd;

  // 这个类的作用是一个线性映射，从一个范围映射到另一个范围
  public LinearScale(T domainStart, T domainEnd, double rangeStart, double rangeEnd) {
    // 这里是输入域的范围
    this.domainStart = domainStart.doubleValue();
    this.domainEnd = domainEnd.doubleValue();

    if (this.domainStart >= this.domainEnd) {
      throw new IllegalArgumentException("domainStart must be less than domainEnd");
    }
    // 这里是输出域的范围
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
  }

  // 这里是将输入阈的一个值映射到输出域的值，公式容易看，写成代码就不太直观了
  public double scale(T value) {
    double val = value.doubleValue();
    if (val < domainStart || val > domainEnd) {
      throw new IllegalArgumentException("Value out of domain range");
    }
    return rangeStart + (rangeEnd - rangeStart) * (val - domainStart) / (domainEnd - domainStart);
  }
}
