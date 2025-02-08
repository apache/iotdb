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

import java.util.List;

public class PatternResult {
  private int id;
  private double match;
  private double size;
  private double matchPos;
  private Long timespan;
  private List<Point> points;
  private double minPos;
  private double maxPos;
  private List<Section> sections;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public List<Point> getPoints() {
    return points;
  }

  public void setPoints(List<Point> points) {
    this.points = points;
  }

  public double getMatch() {
    return match;
  }

  public void setMatch(double match) {
    this.match = match;
  }

  public double getSize() {
    return size;
  }

  public void setSize(double size) {
    this.size = size;
  }

  public double getMatchPos() {
    return matchPos;
  }

  public void setMatchPos(double matchPos) {
    this.matchPos = matchPos;
  }

  public Long getTimespan() {
    return timespan;
  }

  public void setTimespan(Long timespan) {
    this.timespan = timespan;
  }

  public double getMinPos() {
    return minPos;
  }

  public void setMinPos(double minPos) {
    this.minPos = minPos;
  }

  public double getMaxPos() {
    return maxPos;
  }

  public void setMaxPos(double maxPos) {
    this.maxPos = maxPos;
  }

  public List<Section> getSections() {
    return sections;
  }

  public void setSections(List<Section> sections) {
    this.sections = sections;
  }

  @Override
  public String toString() {
    return String.format(
        "{\"distance\":%.2f,\"startTime\":%d,\"endTime\":%d}",
        match, (long) points.get(0).getOrigX(), (long) points.get(points.size() - 1).getOrigX());
  }
}
