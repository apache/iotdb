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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Section {
  private double sign; // enum -1, 0, 1
  private final List<Point> points = new ArrayList<>();

  private double MaxHeight = 0.0;
  private int MaxHeightIndex = 0;
  private double MinHeight = Double.MAX_VALUE;
  private int MinHeightIndex = 0;
  private double MaxWidth = 0.0;
  private double MinWidth = Double.MAX_VALUE;

  private boolean isFinal = false;
  private boolean isVisited = false;

  private int id = 0;

  // only record the SE without divide the height(C)
  private final Map<Integer, Double> calcResult = new HashMap<>();
  private final List<Section> NextSectionList = new ArrayList<>();
  private final List<Section> PrevSectionList = new ArrayList<>();

  public Section(double sign) {
    this.sign = sign;
  }

  public void setSign(double sign) {
    this.sign = sign;
  }

  public double getSign() {
    return sign;
  }

  public void addPoint(Point point) {
    this.points.add(point);
    if (point.y > MaxHeight) {
      MaxHeight = point.y;
      MaxHeightIndex = points.size() - 1;
    }
    if (point.y < MinHeight) {
      MinHeight = point.y;
      MinHeightIndex = points.size() - 1;
    }
    if (point.x > MaxWidth) {
      MaxWidth = point.x;
    }
    if (point.x < MinWidth) {
      MinWidth = point.x;
    }
  }

  public List<Point> getPoints() {
    return points;
  }

  public double getMaxHeight() {
    return MaxHeight;
  }

  public double getMinHeight() {
    return MinHeight;
  }

  public double getHeightBound() {
    return MaxHeight - MinHeight;
  }

  public int getMaxHeightIndex() {
    return MaxHeightIndex;
  }

  public int getMinHeightIndex() {
    return MinHeightIndex;
  }

  public double getMaxWidth() {
    return MaxWidth;
  }

  public double getMinWidth() {
    return MinWidth;
  }

  public double getWidthBound() {
    return MaxWidth - MinWidth;
  }

  public List<Section> getNextSectionList() {
    return NextSectionList;
  }

  public List<Section> getPrevSectionList() {
    return PrevSectionList;
  }

  public void setIsFinal(boolean isFinal) {
    this.isFinal = isFinal;
  }

  public boolean isFinal() {
    return isFinal;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public void setIsVisited(boolean isVisited) {
    this.isVisited = isVisited;
  }

  public boolean isVisited() {
    return isVisited;
  }

  public Map<Integer, Double> getCalcResult() {
    return calcResult;
  }

  public Section copy() {
    Section section = new Section(this.sign);
    for (Point point : this.points) {
      section.addPoint(new Point(point.x, point.y));
    }
    section.MaxHeight = this.MaxHeight;
    section.MinHeight = this.MinHeight;
    section.MaxWidth = this.MaxWidth;
    section.MinWidth = this.MinWidth;
    return section;
  }

  // address while reading data from database, only have sign, points , height, width
  public List<Section> concat(Section section, double smoothValue) {

    List<Section> concatResult = new ArrayList<>();

    double maxHeight = Math.max(this.MaxHeight, section.getMaxHeight());
    double minHeight = Math.min(this.MinHeight, section.getMinHeight());
    double bound = maxHeight - minHeight;
    if (bound <= smoothValue) {
      for (int i = 1; i < section.getPoints().size(); i++) {
        this.addPoint(section.getPoints().get(i));
      }
      concatResult.add(this);
    } else {
      Section firstSection = new Section(this.sign);
      Section midSection;
      Section secondSection = new Section(section.getSign());
      int indexFirst;
      int indexSecond;

      // if maxHeight > this.MaxHeight, claim that the second one is higher than the first one, so
      // it is up state
      if (maxHeight > this.MaxHeight) {
        // find the lower point which has the max x-axis in the first one, and the higher point
        // which has a min x-axis in the second one
        // first one only save the first point to the minHeightIndex
        // mid one start from minHeightIndex to the end of the first one, and the first one to the
        // maxHeightIndex in the second section
        // second one only save the maxHeightIndex to the end
        midSection = new Section(1);
        indexFirst = this.MinHeightIndex;
        indexSecond = section.getMaxHeightIndex();
      }
      // if not, and the bound is bigger claim the minHeight is lower than the first one, so it is
      // down state
      else {
        // find the higher point which has the max x-axis in the first one, and the lower point
        // which has a min x-axis in the second one
        // first one only save the first point to the maxHeightIndex
        // start from maxHeightIndex to the end of the first one, and the first one to the
        // minHeightIndex in the second section
        // second one only save the minHeightIndex to the end
        midSection = new Section(-1);
        indexFirst = this.MaxHeightIndex;
        indexSecond = section.getMinHeightIndex();
      }

      for (int i = 0; i <= indexFirst; i++) {
        firstSection.addPoint(this.points.get(i));
      }
      for (int i = indexFirst; i < this.points.size(); i++) {
        midSection.addPoint(this.points.get(i));
      }
      for (int i = 1; i <= indexSecond; i++) {
        midSection.addPoint(section.getPoints().get(i));
      }
      for (int i = indexSecond; i < section.getPoints().size(); i++) {
        secondSection.addPoint(section.getPoints().get(i));
      }
      if (!firstSection.getPoints().isEmpty()) {
        concatResult.add(firstSection);
      }
      concatResult.add(midSection);
      if (!secondSection.getPoints().isEmpty()) {
        concatResult.add(secondSection);
      }
    }
    return concatResult;
  }

  // address while reading data from database, only have sign, points , height, width
  public void combine(Section section) {
    for (int i = 1; i < section.getPoints().size(); i++) {
      this.addPoint(section.getPoints().get(i));
    }
  }
}
