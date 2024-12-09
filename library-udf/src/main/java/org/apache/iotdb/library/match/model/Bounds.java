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

package org.apache.iotdb.library.match.model;

public class Bounds {
  private Double minX = (double) Long.MAX_VALUE;
  private Double maxX = (double) Long.MIN_VALUE;
  private Double minY = (double) Long.MAX_VALUE;
  private Double maxY = (double) Long.MIN_VALUE;

  public Double getMinX() {
    return minX;
  }

  public void setMinX(Double minX) {
    this.minX = minX;
  }

  public Double getMaxX() {
    return maxX;
  }

  public void setMaxX(Double maxX) {
    this.maxX = maxX;
  }

  public Double getMinY() {
    return minY;
  }

  public void setMinY(Double minY) {
    this.minY = minY;
  }

  public Double getMaxY() {
    return maxY;
  }

  public void setMaxY(Double maxY) {
    this.maxY = maxY;
  }
}
