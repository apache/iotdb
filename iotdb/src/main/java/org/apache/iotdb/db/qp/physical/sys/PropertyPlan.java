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
package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.db.qp.logical.sys.PropertyOperator.PropertyType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.sys.PropertyOperator;

/**
 * Manipulate property plan
 */
public class PropertyPlan extends PhysicalPlan {
    private final PropertyOperator.PropertyType propertyType;
    private Path propertyPath;
    private Path metadataPath;

    public Path getPropertyPath() {
        return propertyPath;
    }

    public Path getMetadataPath() {
        return metadataPath;
    }

    public PropertyOperator.PropertyType getPropertyType() {
        return propertyType;
    }

    public PropertyPlan(PropertyOperator.PropertyType propertyType, Path propertyPath, Path metadataPath) {
        super(false, Operator.OperatorType.PROPERTY);
        this.propertyType = propertyType;
        this.propertyPath = propertyPath;
        this.metadataPath = metadataPath;
    }

    @Override
    public String toString() {
        return "propertyPath: " + propertyPath.toString() + "\nmetadataPath: " + metadataPath + "\npropertyType: "
                + propertyType;
    }

    @Override
    public List<Path> getPaths() {
        List<Path> ret = new ArrayList<>();
        if (metadataPath != null) {
            ret.add(metadataPath);
        }
        if (propertyPath != null) {
            ret.add(propertyPath);
        }
        return ret;
    }
}
