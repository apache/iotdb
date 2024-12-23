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

/* Point definition
 * The x and the y are the values to consider to do the matches, they has been scaled for
 * the current aspect ratio
 * The origX and origY are the real values.
 * For example, if the dataset has values from 1M to 2M they will be scaled from 0 to ~1000
 * to match the same resolution of the canvas where the query is drawn.
 */
public class Point {
  private double x;
  private double y;
  private double origX;
  private double origY;

  public double getX() {
    return x;
  }

  public void setX(double x) {
    this.x = x;
  }

  public double getY() {
    return y;
  }

  public void setY(double y) {
    this.y = y;
  }

  public double getOrigX() {
    return origX;
  }

  public void setOrigX(double origX) {
    this.origX = origX;
  }

  public double getOrigY() {
    return origY;
  }

  public void setOrigY(double origY) {
    this.origY = origY;
  }

  public Point(double x, double y) {
    this.x = x;
    this.y = y;
  }

  public Point(double x, double y, double origX, double origY) {
    this.x = x;
    this.y = y;
    this.origX = origX;
    this.origY = origY;
  }

  public Point copy() {
    return new Point(this.x, this.y, this.origX, this.origY);
  }

  public Point translateXCopy(double offsetX, double offsetOrigX) {
    return new Point(this.x + offsetX, this.y, this.origX + offsetOrigX, this.origY);
  }

  @Override
  public String toString() {
    return "(" + this.x + "," + this.y + ")";
  }
}
