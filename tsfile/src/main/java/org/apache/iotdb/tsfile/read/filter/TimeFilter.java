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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.*;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.operator.*;

public class TimeFilter {

    public static class TimeEq extends Eq {
        private TimeEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeNotEq extends NotEq {
        private TimeNotEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGt extends Gt {
        private TimeGt(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGtEq extends GtEq {
        private TimeGtEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLt extends Lt {
        private TimeLt(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLtEq extends LtEq {
        private TimeLtEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeNotFilter extends NotFilter {
        private TimeNotFilter(Filter filter) {
            super(filter);
        }
    }

    public static TimeEq eq(long value) {
        return new TimeEq(value);
    }

    public static TimeGt gt(long value) {
        return new TimeGt(value);
    }

    public static TimeGtEq gtEq(long value) {
        return new TimeGtEq(value);
    }

    public static TimeLt lt(long value) {
        return new TimeLt(value);
    }

    public static TimeLtEq ltEq(long value) {
        return new TimeLtEq(value);
    }

    public static TimeNotFilter not(Filter filter) {
        return new TimeNotFilter(filter);
    }

    public static TimeNotEq notEq(long value) {
        return new TimeNotEq(value);
    }

}
