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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.process;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.CrossSeriesAggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

public class GroupByTagNode extends MultiChildProcessNode {

  private final List<String> tagKeys;
  private final Map<List<String>, List<CrossSeriesAggregationDescriptor>>
      tagValuesToAggregationDescriptors;
  private final List<String> outputColumnNames;

  // The parameter of `group by time`.
  // Its value will be null if there is no `group by time` clause.
  @Nullable protected GroupByTimeParameter groupByTimeParameter;

  protected Ordering scanOrder;

  public GroupByTagNode(
      PlanNodeId id,
      List<PlanNode> children,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder,
      List<String> tagKeys,
      Map<List<String>, List<CrossSeriesAggregationDescriptor>> tagValuesToAggregationDescriptors,
      List<String> outputColumnNames) {
    super(id, children);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = Validate.notNull(scanOrder);
    this.tagKeys = Validate.notNull(tagKeys);
    this.tagValuesToAggregationDescriptors = Validate.notNull(tagValuesToAggregationDescriptors);
    this.outputColumnNames = Validate.notNull(outputColumnNames);
  }

  public GroupByTagNode(
      PlanNodeId id,
      @Nullable GroupByTimeParameter groupByTimeParameter,
      Ordering scanOrder,
      List<String> tagKeys,
      Map<List<String>, List<CrossSeriesAggregationDescriptor>> tagValuesToAggregationDescriptors,
      List<String> outputColumnNames) {
    super(id);
    this.groupByTimeParameter = groupByTimeParameter;
    this.scanOrder = Validate.notNull(scanOrder);
    this.tagKeys = Validate.notNull(tagKeys);
    this.tagValuesToAggregationDescriptors = Validate.notNull(tagValuesToAggregationDescriptors);
    this.outputColumnNames = Validate.notNull(outputColumnNames);
  }

  @Override
  public PlanNode clone() {
    // TODO: better do deep copy
    return new GroupByTagNode(
        getPlanNodeId(),
        this.groupByTimeParameter,
        this.scanOrder,
        this.tagKeys,
        this.tagValuesToAggregationDescriptors,
        this.outputColumnNames);
  }

  @Override
  public PlanNode createSubNode(int subNodeId, int startIndex, int endIndex) {
    return new HorizontallyConcatNode(
        new PlanNodeId(String.format("%s-%s", getPlanNodeId(), subNodeId)),
        new ArrayList<>(children.subList(startIndex, endIndex)));
  }

  @Override
  public List<String> getOutputColumnNames() {
    List<String> ret = new ArrayList<>(tagKeys);
    ret.addAll(outputColumnNames);
    return ret;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitGroupByTag(this, context);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    // Plan type.
    PlanNodeType.GROUP_BY_TAG.serialize(byteBuffer);

    // Tag keys.
    ReadWriteIOUtils.writeStringList(tagKeys, byteBuffer);

    // Tag values to aggregation descriptors.
    ReadWriteIOUtils.write(tagValuesToAggregationDescriptors.size(), byteBuffer);
    for (Entry<List<String>, List<CrossSeriesAggregationDescriptor>> entry :
        tagValuesToAggregationDescriptors.entrySet()) {
      ReadWriteIOUtils.writeStringList(entry.getKey(), byteBuffer);
      ReadWriteIOUtils.write(entry.getValue().size(), byteBuffer);
      for (CrossSeriesAggregationDescriptor aggregationDescriptor : entry.getValue()) {
        if (aggregationDescriptor == null) {
          ReadWriteIOUtils.write((byte) 0, byteBuffer);
        } else {
          ReadWriteIOUtils.write((byte) 1, byteBuffer);
          aggregationDescriptor.serialize(byteBuffer);
        }
      }
    }

    // Output column names.
    ReadWriteIOUtils.writeStringList(outputColumnNames, byteBuffer);

    // Group by time parameter.
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, byteBuffer);
    } else {
      ReadWriteIOUtils.write((byte) 1, byteBuffer);
      groupByTimeParameter.serialize(byteBuffer);
    }

    // Scan order.
    ReadWriteIOUtils.write(scanOrder.ordinal(), byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    // Plan type.
    PlanNodeType.GROUP_BY_TAG.serialize(stream);

    // Tag keys.
    ReadWriteIOUtils.writeStringList(tagKeys, stream);

    // Tag values to aggregation descriptors.
    ReadWriteIOUtils.write(tagValuesToAggregationDescriptors.size(), stream);
    for (Entry<List<String>, List<CrossSeriesAggregationDescriptor>> entry :
        tagValuesToAggregationDescriptors.entrySet()) {
      ReadWriteIOUtils.writeStringList(entry.getKey(), stream);
      ReadWriteIOUtils.write(entry.getValue().size(), stream);
      for (CrossSeriesAggregationDescriptor aggregationDescriptor : entry.getValue()) {
        if (aggregationDescriptor == null) {
          ReadWriteIOUtils.write((byte) 0, stream);
        } else {
          ReadWriteIOUtils.write((byte) 1, stream);
          aggregationDescriptor.serialize(stream);
        }
      }
    }

    // Output column names.
    ReadWriteIOUtils.writeStringList(outputColumnNames, stream);

    // Group by time parameter.
    if (groupByTimeParameter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      groupByTimeParameter.serialize(stream);
    }

    // Scan order.
    ReadWriteIOUtils.write(scanOrder.ordinal(), stream);
  }

  @Nullable
  public GroupByTimeParameter getGroupByTimeParameter() {
    return groupByTimeParameter;
  }

  public Ordering getScanOrder() {
    return scanOrder;
  }

  public List<String> getTagKeys() {
    return tagKeys;
  }

  /**
   * The CrossSeriesAggregationDescriptor may be null if there exists a key containing no
   * timeSeries.
   *
   * <p>e.g. we have following timeSeries:
   *
   * <ul>
   *   <li>root.sg.d1.s1(k1=v1)
   *   <li>root.sg.d1.s2(k1=v1)
   *   <li>root.sg.d2.s1(k1=v2)
   *   <li>root.sg.d3.s1(k1=v2)
   * </ul>
   *
   * Then the query <code>
   * SELECT avg(s1), avg(s2) FROM root.sg.** GROUP BY TAGS(k1)
   * </code>will generate a {@link GroupByTagNode} with the <code>TagValuesToAggregationDescriptors
   * </code> as below: <code>
   *   {
   *     ["v1"]: [["avg(root.sg.d1.s1)"], ["avg(root.sg.d1.s2)"]],
   *     ["v2"]: [["avg(root.sg.d2.s1)","avg(root.sg.d3.s1)"], null],
   *   }
   * </code>
   *
   * <p>So we should use it carefully with null values.
   */
  public Map<List<String>, List<CrossSeriesAggregationDescriptor>>
      getTagValuesToAggregationDescriptors() {
    return tagValuesToAggregationDescriptors;
  }

  public static GroupByTagNode deserialize(ByteBuffer byteBuffer) {
    // Tag keys.
    List<String> tagKeys = ReadWriteIOUtils.readStringList(byteBuffer);

    // Tag values to aggregation descriptors.
    int numOfEntries = ReadWriteIOUtils.readInt(byteBuffer);
    Map<List<String>, List<CrossSeriesAggregationDescriptor>> tagValuesToAggregationDescriptors =
        new HashMap<>();
    while (numOfEntries > 0) {
      List<String> tagValues = ReadWriteIOUtils.readStringList(byteBuffer);
      List<CrossSeriesAggregationDescriptor> aggregationDescriptors = new ArrayList<>();
      int numOfAggregationDescriptors = ReadWriteIOUtils.readInt(byteBuffer);
      while (numOfAggregationDescriptors > 0) {
        byte isNotNull = ReadWriteIOUtils.readByte(byteBuffer);
        if (isNotNull == 1) {
          aggregationDescriptors.add(CrossSeriesAggregationDescriptor.deserialize(byteBuffer));
        } else {
          aggregationDescriptors.add(null);
        }
        numOfAggregationDescriptors -= 1;
      }
      tagValuesToAggregationDescriptors.put(tagValues, aggregationDescriptors);
      numOfEntries -= 1;
    }

    // Output column names.
    List<String> outputColumnNames = ReadWriteIOUtils.readStringList(byteBuffer);

    // Group by time parameter.
    byte hasGroupByTimeParameter = ReadWriteIOUtils.readByte(byteBuffer);
    GroupByTimeParameter groupByTimeParameter = null;
    if (hasGroupByTimeParameter == 1) {
      groupByTimeParameter = GroupByTimeParameter.deserialize(byteBuffer);
    }

    // Scan order.
    Ordering scanOrder = Ordering.values()[ReadWriteIOUtils.readInt(byteBuffer)];

    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new GroupByTagNode(
        planNodeId,
        groupByTimeParameter,
        scanOrder,
        tagKeys,
        tagValuesToAggregationDescriptors,
        outputColumnNames);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GroupByTagNode that = (GroupByTagNode) o;
    return Objects.equals(groupByTimeParameter, that.groupByTimeParameter)
        && scanOrder == that.scanOrder
        && Objects.equals(tagKeys, that.tagKeys)
        && Objects.equals(tagValuesToAggregationDescriptors, that.tagValuesToAggregationDescriptors)
        && Objects.equals(outputColumnNames, that.outputColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        groupByTimeParameter,
        scanOrder,
        tagKeys,
        tagValuesToAggregationDescriptors,
        outputColumnNames);
  }

  @Override
  public String toString() {
    return String.format(
        "GroupByTagNode-%s: Output: %s, Input: %s",
        getPlanNodeId(),
        getOutputColumnNames(),
        tagValuesToAggregationDescriptors.values().stream()
            .flatMap(
                list -> list.stream().map(CrossSeriesAggregationDescriptor::getInputExpressions))
            .collect(Collectors.toList()));
  }
}
