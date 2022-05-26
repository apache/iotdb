/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterSerializeId;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Between<T extends Comparable<T>> implements Filter {

    private static final long serialVersionUID = -537390606419370764L;

    protected T secondOperator;

    protected T thirdOperator;

    protected boolean not;

    protected FilterType filterType;

    public Between() {
    }

    public Between(T secondOperator, T thirdOperator, FilterType filterType, boolean not) {
        this.secondOperator = secondOperator;
        this.thirdOperator = thirdOperator;
        this.not = not;
    }

    @Override
    public void serialize(DataOutputStream outputStream) {
        try {
            outputStream.write(getSerializeId().ordinal());
            outputStream.write(filterType.ordinal());
            ReadWriteIOUtils.write(not, outputStream);
            ReadWriteIOUtils.writeObject(secondOperator, outputStream);
            ReadWriteIOUtils.writeObject(thirdOperator, outputStream);
        } catch (IOException ignored) {
            // ignored
        }
    }

    @Override
    public void deserialize(ByteBuffer buffer) {
        filterType = FilterType.values()[buffer.get()];
        not = ReadWriteIOUtils.readBool(buffer);
        secondOperator = (T)ReadWriteIOUtils.readObject(buffer);
        thirdOperator = (T)ReadWriteIOUtils.readObject(buffer);
    }

    @Override
    public FilterSerializeId getSerializeId() {
        return FilterSerializeId.BETWEEN;
    }

    @Override
    public boolean satisfy(Statistics statistics) {
        return true;
    }

    @Override
    public boolean satisfy(long time, Object value) {
        Object v = filterType == FilterType.TIME_FILTER ? time : value;
        return secondOperator.compareTo((T) v) <= 0 && thirdOperator.compareTo((T) v) >= 0;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        return false;
    }

    @Override
    public boolean containStartEndTime(long startTime, long endTime) {
        return false;
    }

    @Override
    public Filter copy() {
        return new Between(secondOperator, thirdOperator, filterType, not);
    }

}
