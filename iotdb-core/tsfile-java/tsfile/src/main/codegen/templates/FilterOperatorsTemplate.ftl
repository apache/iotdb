<@pp.dropOutputFile />
<#list filters as filter>
  <#assign className = "${filter.javaBoxName}FilterOperators">
  <#assign filterName = "${filter.javaBoxName}Filter">
  <@pp.changeOutputFile name="/org/apache/tsfile/read/filter/operator/${className}.java" />
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

package org.apache.tsfile.read.filter.operator;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.CANNOT_PUSH_DOWN_MSG;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.basic.${filter.javaBoxName}Filter;
import org.apache.tsfile.read.filter.basic.OperatorType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
<#if filter.dataType != "boolean">
import java.util.Collections;
</#if>
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

  /*
* This class is generated using freemarker and the ${.template_name} template.
*/
public final class ${className} {

  private ${className}() {
    // forbidden construction
  }

  private static final String CONSTANT_CANNOT_BE_NULL_MSG = "constant cannot be null";

  private static final String OPERATOR_TO_STRING_FORMAT = "measurements[%s] %s %s";

  // base class for ValueEq, ValueNotEq, ValueLt, ValueGt, ValueLtEq, ValueGtEq
  abstract static class ValueColumnCompareFilter extends ${filterName} {

    protected final ${filter.dataType} constant;

    protected ValueColumnCompareFilter(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex);
      <#if filter.dataType == "Binary">
      this.constant = Objects.requireNonNull(constant, CONSTANT_CANNOT_BE_NULL_MSG);
      <#else>
      this.constant = constant;
      </#if>
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnCompareFilter(ByteBuffer buffer) {
      super(buffer);
      <#if filter.dataType == "Binary">
      this.constant = Objects.requireNonNull(ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer), CONSTANT_CANNOT_BE_NULL_MSG);
      <#else>
      this.constant = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
      </#if>
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(constant, outputStream);
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
      ValueColumnCompareFilter that = (ValueColumnCompareFilter) o;
      <#if filter.dataType == "Binary">
      return Objects.equals(constant,that.constant);
      <#else>
      return constant == that.constant;
      </#if>
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), constant);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), constant);
    }
  }

  public static final class ValueEq extends ValueColumnCompareFilter {

    public ValueEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return constant.equals(value);
      <#else>
      return constant == value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) < 0
          || constant.compareTo((${filter.dataType}) statistics.getMaxValue()) > 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      return constant < (${filter.javaBoxName}) statistics.getMinValue()
          || constant > (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) == 0
          && constant.compareTo((${filter.dataType}) statistics.getMaxValue()) == 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      return constant == (${filter.javaBoxName}) statistics.getMinValue()
          && constant == (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueNotEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_EQ;
    }
  }

  public static final class ValueNotEq extends ValueColumnCompareFilter {

    public ValueNotEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueNotEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return !constant.equals(value);
      <#else>
      return constant != value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) == 0
          && constant.compareTo((${filter.dataType}) statistics.getMaxValue()) == 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      // drop if this is a column where min = max = value
      return constant == (${filter.javaBoxName}) statistics.getMinValue()
          && constant == (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) < 0
          || constant.compareTo((${filter.dataType}) statistics.getMaxValue()) > 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return constant < (${filter.javaBoxName}) statistics.getMinValue()
          || constant > (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NEQ;
    }
  }

  public static final class ValueLt extends ValueColumnCompareFilter {

    public ValueLt(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueLt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) > 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) > 0;
      <#else>
      return constant > value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) <= 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      // drop if value <= min
      return constant <= (${filter.javaBoxName}) statistics.getMinValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMaxValue()) > 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return constant > (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueGtEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LT;
    }
  }

  public static final class ValueLtEq extends ValueColumnCompareFilter {

    public ValueLtEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueLtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) >= 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) >= 0;
      <#else>
      return constant >= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) < 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      // drop if value < min
      return constant < (${filter.javaBoxName}) statistics.getMinValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMaxValue()) >= 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return constant >= (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueGt(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LTEQ;
    }
  }

  public static final class ValueGt extends ValueColumnCompareFilter {

    public ValueGt(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueGt(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) < 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) < 0;
      <#else>
      return constant < value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMaxValue()) >= 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      // drop if value >= max
      return constant >= (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) < 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return constant < (${filter.javaBoxName}) statistics.getMinValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueLtEq(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GT;
    }
  }

  public static final class ValueGtEq extends ValueColumnCompareFilter {

    public ValueGtEq(int measurementIndex, ${filter.dataType} constant) {
      super(measurementIndex, constant);
    }

    public ValueGtEq(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(constant,value) <= 0;
      <#elseif filter.dataType == "Binary">
      return constant.compareTo(value) <= 0;
      <#else>
      return constant <= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMaxValue()) > 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      // drop if value > max
      return constant > (${filter.javaBoxName}) statistics.getMaxValue();
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return constant.compareTo((${filter.dataType}) statistics.getMinValue()) <= 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return constant <= (${filter.javaBoxName}) statistics.getMinValue();
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueLt(measurementIndex, constant);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_GTEQ;
    }
  }

  // base class for ValueBetweenAnd, ValueNotBetweenAnd
  abstract static class ValueColumnRangeFilter extends ${filterName} {

    protected final ${filter.dataType} min;
    protected final ${filter.dataType} max;

    protected ValueColumnRangeFilter(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex);
      <#if filter.dataType == "Binary">
      this.min = Objects.requireNonNull(min,"min cannot be null");
      this.max = Objects.requireNonNull(max,"max cannot be null");
      <#else>
      this.min = min;
      this.max = max;
      </#if>
    }

    @SuppressWarnings("unchecked")
    protected ValueColumnRangeFilter(ByteBuffer buffer) {
      super(buffer);
      <#if filter.dataType == "Binary">
      this.min = Objects.requireNonNull(ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer),"min cannot be null");
      this.max = Objects.requireNonNull(ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer),"max cannot be null");
      <#else>
      this.min = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
      this.max = ReadWriteIOUtils.read${filter.dataType?cap_first}(buffer);
      </#if>
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(min, outputStream);
      ReadWriteIOUtils.write(max, outputStream);
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
      ValueColumnRangeFilter that = (ValueColumnRangeFilter) o;
      <#if filter.dataType == "Binary">
      return min.equals(that.min) && max.equals(that.max);
      <#else>
      return min == that.min && max == that.max;
      </#if>
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), min, max);
    }

    @Override
    public String toString() {
      return String.format(
          "measurements[%s] %s %s AND %s",
          measurementIndex, getOperatorType().getSymbol(), min, max);
    }
  }

  public static final class ValueBetweenAnd extends ValueColumnRangeFilter {

    public ValueBetweenAnd(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex, min, max);
    }

    public ValueBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(min,value) <= 0
          && Boolean.compare(max,value) >= 0;
      <#elseif filter.dataType == "Binary">
      return min.compareTo(value) <= 0
          && max.compareTo(value) >= 0;
      <#else>
      return min <= value && max >= value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return ((${filter.dataType}) statistics.getMaxValue()).compareTo(min) < 0
          || ((${filter.dataType}) statistics.getMinValue()).compareTo(max) > 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      return (${filter.javaBoxName}) statistics.getMaxValue() < min
          || (${filter.javaBoxName}) statistics.getMinValue() > max;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return ((${filter.dataType}) statistics.getMinValue()).compareTo(min) >= 0
          && ((${filter.dataType}) statistics.getMaxValue()).compareTo(max) <= 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return (${filter.javaBoxName}) statistics.getMinValue() >= min
          && (${filter.javaBoxName}) statistics.getMaxValue() <= max;
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueNotBetweenAnd(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_BETWEEN_AND;
    }
  }

  public static final class ValueNotBetweenAnd extends ValueColumnRangeFilter {

    public ValueNotBetweenAnd(int measurementIndex, ${filter.dataType} min, ${filter.dataType} max) {
      super(measurementIndex, min, max);
    }

    public ValueNotBetweenAnd(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean valueSatisfy(Object value){
      return valueSatisfy((${filter.dataType}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "boolean">
      return Boolean.compare(min,value) > 0
          || Boolean.compare(max,value) < 0;
      <#elseif filter.dataType == "Binary">
      return min.compareTo(value) > 0 || max.compareTo(value) < 0;
      <#else>
      return min > value || max < value;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean canSkip(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return ((${filter.dataType}) statistics.getMinValue()).compareTo(min) >= 0
          && ((${filter.dataType}) statistics.getMaxValue()).compareTo(max) <= 0;
        <#else>
      return false;
        </#if>
      <#else>
      // drop if value < min || value > max
      if(statistics.isEmpty()){
        return false;
      }
      return (${filter.javaBoxName}) statistics.getMinValue() >= min
          && (${filter.javaBoxName}) statistics.getMaxValue() <= max;
      </#if>
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      <#if filter.dataType == "boolean" || filter.dataType == "Binary">
        <#if filter.javaBoxName == "String">
      if(statistics.isEmpty()){
        return false;
      }
      return ((${filter.dataType}) statistics.getMinValue()).compareTo(max) > 0
          || ((${filter.dataType}) statistics.getMaxValue()).compareTo(min) < 0;
        <#else>
      return false;
        </#if>
      <#else>
      if(statistics.isEmpty()){
        return false;
      }
      return (${filter.javaBoxName}) statistics.getMinValue() > max
          || (${filter.javaBoxName}) statistics.getMaxValue() < min;
      </#if>
    }

    @Override
    public Filter reverse() {
      return new ValueBetweenAnd(measurementIndex, min, max);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_BETWEEN_AND;
    }
  }

  // base class for ValueIn, ValueNotIn
  abstract static class ValueColumnSetFilter extends ${filterName} {

    <#if filter.javaBoxName == "String">
    protected final Set<${filter.dataType}> candidates;
    <#else>
    protected final Set<${filter.javaBoxName}> candidates;
    </#if>

    protected final ${filter.dataType} candidatesMin;
    protected final ${filter.dataType} candidatesMax;

    <#if filter.javaBoxName == "String">
    protected ValueColumnSetFilter(int measurementIndex, Set<${filter.dataType}> candidates) {
    <#else>
    protected ValueColumnSetFilter(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
    </#if>
      super(measurementIndex);
      this.candidates = candidates;

      <#if filter.javaBoxName == "String">
      Set<${filter.dataType}> filteredSet = candidates.stream().filter(Objects::nonNull).collect(Collectors.toSet());
      <#else>
      Set<${filter.javaBoxName}> filteredSet = candidates.stream().filter(Objects::nonNull).collect(Collectors.toSet());
      </#if>
      <#if filter.dataType == "boolean">
      // BooleanStatistics is not available
      this.candidatesMin = false;
      this.candidatesMax = false;
      <#elseif filter.dataType == "Binary">
      this.candidatesMin = !filteredSet.isEmpty() ? Collections.min(filteredSet) : null;
      this.candidatesMax = !filteredSet.isEmpty() ? Collections.max(filteredSet) : null;
      <#else>
      this.candidatesMin = !filteredSet.isEmpty() ? Collections.min(filteredSet) : ${filter.javaBoxName}.MIN_VALUE;
      this.candidatesMax = !filteredSet.isEmpty() ? Collections.max(filteredSet) : ${filter.javaBoxName}.MAX_VALUE;
      </#if>
    }

    protected ValueColumnSetFilter(ByteBuffer buffer) {
      super(buffer);
      boolean hasNull = ReadWriteIOUtils.readBoolean(buffer);
      <#if filter.javaBoxName == "String">
      this.candidates = ReadWriteIOUtils.read${filter.dataType}Set(buffer);
      <#else>
      this.candidates = ReadWriteIOUtils.read${filter.javaBoxName}Set(buffer);
      </#if>
      <#if filter.dataType == "boolean">
      // BooleanStatistics is not available
      this.candidatesMin = false;
      this.candidatesMax = false;
      <#elseif filter.dataType == "Binary">
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : null;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : null;
      <#else>
      this.candidatesMin = !candidates.isEmpty() ? Collections.min(candidates) : ${filter.javaBoxName}.MAX_VALUE;
      this.candidatesMax = !candidates.isEmpty() ? Collections.max(candidates) : ${filter.javaBoxName}.MAX_VALUE;
      </#if>
      if(hasNull){
        this.candidates.add(null);
      }
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(candidates.contains(null), outputStream);
      <#if filter.javaBoxName == "String">
      ReadWriteIOUtils.write${filter.dataType}Set(candidates, outputStream);
      <#else>
      ReadWriteIOUtils.write${filter.javaBoxName}Set(candidates, outputStream);
      </#if>
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
      ValueColumnSetFilter that = (ValueColumnSetFilter) o;
      return candidates.equals(that.candidates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), candidates);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), candidates);
    }
  }

  public static final class ValueIn extends ValueColumnSetFilter {

    <#if filter.javaBoxName == "String">
    public ValueIn(int measurementIndex, Set<${filter.dataType}> candidates) {
      super(measurementIndex, candidates);
    }
    <#else>
    public ValueIn(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
      super(measurementIndex, candidates);
    }
    </#if>

    public ValueIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return candidates.contains((${filter.javaBoxName}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      return candidates.contains(value);
    }

    @Override
    public boolean canSkip(IMetadata metadata) {
      <#if filter.dataType == "boolean" || (filter.dataType == "Binary" && filter.javaBoxName != "String")>
      return false;
      <#else>
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      // All values are null, but candidates do not contain null
      if ((!statistics.isPresent() || isAllNulls(statistics.get())) && !candidates.isEmpty()) {
        return true;
      }

      // All values are not null, but candidate is one null value
      if (!metadata.hasNullValue(measurementIndex) && candidates.isEmpty()) {
        return true;
      }

      if (statistics.isPresent()) {
        Statistics<? extends Serializable> stat = statistics.get();
        <#if filter.dataType == "Binary" && filter.javaBoxName == "String">
        ${filter.dataType} valuesMin = (${filter.dataType}) stat.getMinValue();
        ${filter.dataType} valuesMax = (${filter.dataType}) stat.getMaxValue();
        <#else>
        ${filter.javaBoxName} valuesMin = (${filter.javaBoxName}) stat.getMinValue();
        ${filter.javaBoxName} valuesMax = (${filter.javaBoxName}) stat.getMaxValue();
        </#if>
        // All values are same
        if (valuesMin.equals(valuesMax)) {
          return !candidates.contains(valuesMin);
        } else {
          if (!candidates.isEmpty()) {
            // All values are less than min, or greater than max
            <#if filter.dataType == "Binary" && filter.javaBoxName == "String">
            return candidatesMin.compareTo(valuesMax) > 0
                || candidatesMax.compareTo(valuesMin) < 0;
            <#else>
            return candidatesMin > valuesMax || candidatesMax < valuesMin;
            </#if>
          }
        }
      }

      return false;
      </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public boolean allSatisfy(IMetadata metadata) {
      <#if filter.dataType == "boolean" || (filter.dataType == "Binary" && filter.javaBoxName != "String")>
      return false;
      <#else>
      Optional<Statistics<? extends Serializable>> statistics =
          metadata.getMeasurementStatistics(measurementIndex);

      // All values are null, and candidate contains null
      // Note null value cannot be added to set
      if ((!statistics.isPresent() || isAllNulls(statistics.get())) && candidates.isEmpty()) {
        return true;
      }

      // has null value, just return false
      if (metadata.hasNullValue(measurementIndex)) {
        return false;
      }

      // All values are same
      if (statistics.isPresent()) {
        Statistics<? extends Serializable> stat = statistics.get();
        <#if filter.dataType == "Binary" && filter.javaBoxName == "String">
        ${filter.dataType} valuesMin = (${filter.dataType}) stat.getMinValue();
        ${filter.dataType} valuesMax = (${filter.dataType}) stat.getMaxValue();
        <#else>
        ${filter.javaBoxName} valuesMin = (${filter.javaBoxName}) stat.getMinValue();
        ${filter.javaBoxName} valuesMax = (${filter.javaBoxName}) stat.getMaxValue();
        </#if>
        // All values are same
        if (valuesMin.equals(valuesMax)) {
          return candidates.contains(valuesMin);
        }
      }

      return false;
      </#if>
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      throw new NotImplementedException();
    }

    @Override
    public Filter reverse() {
      return new ValueNotIn(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_IN;
    }

    private boolean isAllNulls(Statistics<? extends Serializable> statistics) {
      return statistics.getCount() == 0;
    }

    private static boolean statisticsNotAvailable(Statistics<?> statistics) {
      return statistics.getType() == TSDataType.TEXT
          || statistics.getType() == TSDataType.BOOLEAN
          || statistics.getType() == TSDataType.BLOB
          || statistics.isEmpty();
    }
  }

  public static final class ValueNotIn extends ValueColumnSetFilter {

    <#if filter.javaBoxName == "String">
    public ValueNotIn(int measurementIndex, Set<${filter.dataType}> candidates) {
      super(measurementIndex, candidates);
    }
    <#else>
    public ValueNotIn(int measurementIndex, Set<${filter.javaBoxName}> candidates) {
      super(measurementIndex, candidates);
    }
    </#if>

    public ValueNotIn(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !candidates.contains((${filter.javaBoxName}) value);
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      return !candidates.contains(value);
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueIn(measurementIndex, candidates);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_IN;
    }
  }

  // base class for ValueRegex, ValueNotRegex
  abstract static class ValueColumnPatternMatchFilter extends ${filterName} {

    protected final Pattern pattern;

    protected ValueColumnPatternMatchFilter(int measurementIndex, Pattern pattern) {
      super(measurementIndex);
      this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
    }

    protected ValueColumnPatternMatchFilter(ByteBuffer buffer) {
      super(buffer);
      this.pattern =
      Pattern.compile(
        Objects.requireNonNull(
        ReadWriteIOUtils.readString(buffer), "pattern cannot be null"));
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(pattern.pattern(), outputStream);
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
      ValueColumnPatternMatchFilter that = (ValueColumnPatternMatchFilter) o;
      return pattern.pattern().equals(that.pattern.pattern());
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pattern.pattern());
    }

    @Override
    public String toString() {
      return String.format(
      OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), pattern);
    }
  }

  public static final class ValueRegexp extends ValueColumnPatternMatchFilter {

    public ValueRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
    <#if filter.dataType == "Binary">
      return pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    <#else>
      return pattern.matcher(new MatcherInput(String.valueOf(value), new AccessCount())).find();
    </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_REGEXP;
    }
  }

  public static final class ValueNotRegexp extends ValueColumnPatternMatchFilter {

    public ValueNotRegexp(int measurementIndex, Pattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueNotRegexp(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
    <#if filter.dataType == "Binary">
      return !pattern.matcher(new MatcherInput(value.toString(), new AccessCount())).find();
    <#else>
      return !pattern.matcher(new MatcherInput(String.valueOf(value), new AccessCount())).find();
    </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueRegexp(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_REGEXP;
    }
  }

  // base class for ValueLike, ValueNotLike
  abstract static class ValueColumnPatternLikeMatchFilter extends ${filterName} {

    protected final LikePattern pattern;

    protected ValueColumnPatternLikeMatchFilter(int measurementIndex, LikePattern pattern) {
      super(measurementIndex);
      this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
    }

    protected ValueColumnPatternLikeMatchFilter(ByteBuffer buffer) {
      super(buffer);
      this.pattern =
          LikePattern.compile(
              ReadWriteIOUtils.readString(buffer),
              ReadWriteIOUtils.readBool(buffer)
                  ? Optional.of(ReadWriteIOUtils.readString(buffer).charAt(0))
                  : Optional.empty());
    }

    @Override
    public void serialize(DataOutputStream outputStream) throws IOException {
      super.serialize(outputStream);
      ReadWriteIOUtils.write(pattern.getPattern(), outputStream);
      if(pattern.getEscape().isPresent()){
        ReadWriteIOUtils.write(true, outputStream);
        ReadWriteIOUtils.write(pattern.getEscape().get().toString(), outputStream);
      }
      else{
        ReadWriteIOUtils.write(false, outputStream);
      }
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
      ValueColumnPatternLikeMatchFilter that = (ValueColumnPatternLikeMatchFilter) o;
      return pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), pattern);
    }

    @Override
    public String toString() {
      return String.format(
          OPERATOR_TO_STRING_FORMAT, measurementIndex, getOperatorType().getSymbol(), pattern);
    }
  }

  public static final class ValueLike extends ValueColumnPatternLikeMatchFilter {

    public ValueLike(int measurementIndex, LikePattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueLike(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return pattern.getMatcher().match(value.toString().getBytes());
      <#else>
      return pattern.getMatcher().match(String.valueOf(value).getBytes());
      </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueNotLike(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_LIKE;
    }
  }

  public static final class ValueNotLike extends ValueColumnPatternLikeMatchFilter {

    public ValueNotLike(int measurementIndex, LikePattern pattern) {
      super(measurementIndex, pattern);
    }

    public ValueNotLike(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    public boolean valueSatisfy(Object value){
      return !pattern.getMatcher().match(value.toString().getBytes());
    }

    @Override
    public boolean valueSatisfy(${filter.dataType} value) {
      <#if filter.dataType == "Binary">
      return !pattern.getMatcher().match(value.toString().getBytes());
      <#else>
      return !pattern.getMatcher().match(String.valueOf(value).getBytes());
      </#if>
    }

    @Override
    protected boolean canSkip(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    protected boolean allSatisfy(Statistics<? extends Serializable> statistics) {
      return false;
    }

    @Override
    public Filter reverse() {
      return new ValueLike(measurementIndex, pattern);
    }

    @Override
    public OperatorType getOperatorType() {
      return OperatorType.VALUE_NOT_LIKE;
    }
  }

  private static class AccessCount {
    private int count;
    private final int accessThreshold =
        TSFileDescriptor.getInstance().getConfig().getPatternMatchingThreshold();

    public void check() throws IllegalStateException {
      if (this.count++ > accessThreshold) {
        throw new IllegalStateException("Pattern access threshold exceeded");
      }
    }
  }

  private static class MatcherInput implements CharSequence {

    private final CharSequence value;

    private final AccessCount access;

    public MatcherInput(CharSequence value, AccessCount access) {
      this.value = value;
      this.access = access;
    }

    @Override
    public char charAt(int index) {
      this.access.check();
      return this.value.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new MatcherInput(this.value.subSequence(start, end), this.access);
    }

    @Override
    public int length() {
      return this.value.length();
    }

    @Override
    public String toString() {
      return this.value.toString();
    }
  }
}
</#list>