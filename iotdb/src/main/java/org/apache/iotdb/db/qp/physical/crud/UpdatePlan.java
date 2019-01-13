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
package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.db.qp.logical.Operator;

import static org.apache.iotdb.db.qp.constant.SQLConstant.lineFeedSignal;

public class UpdatePlan extends PhysicalPlan {
    private List<Pair<Long, Long>> intervals = new ArrayList<>();
    private String value;
    private Path path;

    public UpdatePlan() {
        super(false, Operator.OperatorType.UPDATE);
    }

    public UpdatePlan(long startTime, long endTime, String value, Path path) {
        super(false, Operator.OperatorType.UPDATE);
        setValue(value);
        setPath(path);
        addInterval(new Pair<>(startTime, endTime));
    }

    public UpdatePlan(List<Pair<Long, Long>> list, String value, Path path) {
        super(false, Operator.OperatorType.UPDATE);
        setValue(value);
        setPath(path);
        intervals = list;
    }

    public List<Pair<Long, Long>> getIntervals() {
        return intervals;
    }

    public void addInterval(Pair<Long, Long> interval) {
        this.intervals.add(interval);
    }

    public void addIntervals(List<Pair<Long, Long>> intervals) {
        this.intervals.addAll(intervals);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (path != null) {
            ret.add(path);
        }
        return ret;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer();
        String preSpace = "  ";
        sc.addTail("UpdatePlan:");
        sc.addTail(preSpace, "paths:  ", path.toString(), lineFeedSignal);
        sc.addTail(preSpace, "value:", value, lineFeedSignal);
        sc.addTail(preSpace, "filter: ", lineFeedSignal);
        intervals.forEach(p -> sc.addTail(preSpace, preSpace, p.left, p.right, lineFeedSignal));
        return sc.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdatePlan that = (UpdatePlan) o;
        return Objects.equals(intervals, that.intervals) && Objects.equals(value, that.value)
                && Objects.equals(path, that.path);
    }
}
