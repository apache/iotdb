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
package org.apache.iotdb.commons.schema.tree;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

// List<String>, String
// Modif
public class PatternTreeMap<V>{
    private PathPatternNode root;
    private Map<PartialPath,Collection<V>> map;
    private BiConsumer<? extends V,? extends Collection<V>> remappingFunction;


    public PatternTreeMap(BiConsumer<? extends V,? extends Collection<V>> remappingFunction) {
        this.root = new PathPatternNode(SQLConstant.ROOT);
        this.map = new ConcurrentHashMap<>();
        this.remappingFunction = remappingFunction;

    }

//    public PatternTreeMap() {
//        this((newValue,set)-> set.add(newValue));
//    }

    public void appendPathPattern(PartialPath pathPattern, V value){


    }

    public void deletePathPattern(PartialPath pathPattern, V value){

    }

    List<V> getOverlappedPathPatterns(PartialPath fullPath){
        return null;
    }
}
