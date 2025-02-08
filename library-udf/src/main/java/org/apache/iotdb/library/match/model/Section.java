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

import java.util.ArrayList;
import java.util.List;

public class Section {
  // the sign of the section 1, -1 or 0
  private double sign;
  // the array of points of that sections,
  private List<Point> points;
  // the array of the tangents contained in the section
  private List<Double> tangents;
  // array of sections that come after (in case of repetitions the next could be a previous one)
  private List<SectionNext> next;

  private int id;

  public double getSign() {
    return sign;
  }

  public void setSign(double sign) {
    this.sign = sign;
  }

  public List<Point> getPoints() {
    return points;
  }

  public void setPoints(List<Point> points) {
    this.points = points;
  }

  public List<Double> getTangents() {
    return tangents;
  }

  public void setTangents(List<Double> tangents) {
    this.tangents = tangents;
  }

  public List<SectionNext> getNext() {
    return next;
  }

  public void setNext(List<SectionNext> next) {
    this.next = next;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public Section(double sign) {
    this.sign = sign;
    this.points = new ArrayList<>();
    this.tangents = new ArrayList<>();
    this.next = new ArrayList<>();
  }

  /* add all the points of a section in the current section */
  public void concat(Section section) {
    for (Point point : section.getPoints()) {
      this.points.add(point);
    }
    for (Double tangent : section.getTangents()) {
      this.tangents.add(tangent);
    }
  }

  /** section copy, but where the tangents are not copied, and the next array is set to null * */
  public Section translateXCopy(double offsetX, double offsetOrigX) {
    Section ns = new Section(this.sign);
    ns.setTangents(this.tangents);
    ns.setId(this.getId());
    for (Point point : this.getPoints()) {
      ns.getPoints().add(point.translateXCopy(offsetX, offsetOrigX));
    }
    return ns;
  }

  /** section copy, but where the tangents are not copied, and the next array is set to null * */
  public Section copy() {
    Section ns = new Section(this.sign);
    ns.setTangents(this.tangents);
    ns.setId(this.getId());
    for (Point point : this.getPoints()) {
      ns.getPoints().add(point.copy());
    }
    return ns;
  }

  public double size() {
    return this.getPoints().get(this.getPoints().size() - 1).getX()
        - this.getPoints().get(0).getX();
  }

  public double sizeEucl() {
    return Math.sqrt(
        Math.pow(
                this.getPoints().get(this.getPoints().size() - 1).getX()
                    - this.getPoints().get(0).getX(),
                2)
            + Math.pow(
                this.getPoints().get(this.getPoints().size() - 1).getY()
                    - this.getPoints().get(0).getY(),
                2));
  }
}
