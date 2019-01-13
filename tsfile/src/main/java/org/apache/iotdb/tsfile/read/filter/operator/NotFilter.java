/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

import java.io.Serializable;

/**
 * NotFilter necessary. Use InvertExpressionVisitor
 */
public class NotFilter implements Filter, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public NotFilter(Filter that) {
        this.that = that;
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return !that.satisfy(digest);
    }

    @Override
    public boolean satisfy(long time, Object value) {
        return !that.satisfy(time, value);
    }

    /**
     * Notice that, if the not filter only contains value filter, this method may return false, this may cause
     * misunderstanding.
     */
    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        return !that.satisfyStartEndTime(startTime, endTime);
    }

    public Filter getFilter() {
        return this.that;
    }

    @Override
    public String toString() {
        return "NotFilter: " + that;
    }

}
