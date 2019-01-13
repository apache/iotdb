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
package org.apache.iotdb.db.sql.parse;

import java.util.List;

/**
 * This interface defines the functions needed by the walkers and dispatchers. These are implemented by the node of the
 * graph that needs to be walked.
 */
public interface Node {

    /**
     * Gets the vector of children nodes. This is used in the graph walker algorithms.
     * 
     * @return List<? extends Node>
     */
    List<? extends Node> getChildren();

    /**
     * Gets the name of the node. This is used in the rule dispatchers.
     * 
     * @return String
     */
    String getName();
}
