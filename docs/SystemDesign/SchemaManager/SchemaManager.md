<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Metadata Management

Metadata of IoTDB is managed by MManger, including:

* MTree
* Log management of metadata
* Tag/Attribute management


## MManager

* Maintain an inverted index for tag: `Map<String, Map<String, Set<LeafMNode>>> tagIndex`

	> tag key -> tag value -> timeseries LeafMNode

In the process of initializing, MManager will replay the mlog to load the metadata into memory. There are six types of operation log:
> At the beginning of each operation, it will try to obatin the write lock of MManager, and release it after operation.

* Create Timeseries
    * check if the storage group exists, if not and the auto create is enable, create it.
    * create a leafMNode in the MTree with alias
	* if the dynamic parameter is enable, check if the memory is satisfied
	* if not restart
	    * persist tags/attributes into tlog, and return the offset
		* set the offset of the leafMNode
		* persist the log into mlog
	* if restart
		* read tlog using offset in mlog, rebuilding the tag inverted index

* Delete Timeseries
    * obtain fullPath list of timeseries satisfying the prefix path
    * iterate the fullPath list, and delete them in MTree
		* before deleting, we need to obtain the parent node's write lock
		* if succeed
			* delete the LeafMNode
			* read tlog using offset in the LeafMNode, update tag inverted index
			* if the storage group becomes empty after deleting, record its name
		* if failed
			* return the full path of failed timeseries
	* if not restart
	   * delete the recorded empty storage group
		* persist log into mlog
		* currently, we won't delete the tag/attribute info of that timeseries in tlog
	

* Set Storage Group
    * add StorageGroupMNode in MTree
	* if dynamic parameter is enable, check if the memory is satisfied
	* if not restart, persist log into mlog

* Delete Storage Group
    * delete the StorageGroupMNode in MTree, and return all the LeafMNode in that storage group
		* While deleting StorageGroupMNode, we need to obtain the write lock of that StorageGroupMNode
		* if succeed
			* delete that StorageGroupMNode
		* if failed
			* return the failed storage group name
	* iterate the returned LeafMNode list, reading the tlog using the offset in LeafMNode, and then update tag inverted index
	* if not restart, persist log into mlog

* Set TTL
    * obtain the corresponding StorageGroupMNode, modify the TTL property in it.
	* if not restart, persist log into mlog

* Change the offset of Timeseries
	* modify the offset of the timeseries's LeafMNode


In addition to these six operation that are needed to be logged, there are another six alter operation to tag/attribute info of timeseries.
 
> Same as above, at the beginning of each operation, it will try to obatin the write lock of MManager, and release it after operation.

* Rename Tag/Attribute
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* if the new name has existed, then throw exception
	* otherwise:
		* if the old name does not exist, then throw exception
		* otherwise, replace the old one with the new name, and persist it into tlog
		* if the old one is tag, we still need to update tag inverted index

* reset tag/attribute value
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* if the key does not exist, then throw exception
	* if the reset one is tag, we still need to update tag inverted index
	* persist the updated tag and attribute information into tlog

* drop existing tag/attribute
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* iterate the tags or attributes needed to be dropped, if it doesn't exist, then skip it, otherwise, drop it
	* if the drooped one is tag, we still need to update tag inverted index
	* persist the updated tag and attribute information into tlog

* add new tags
	* obtain the LeafMNode of that timeseries
	* read tag information through the offset in LeafMNode
	* iterate the tags needed to be added, if it has existed, then throw exception, otherwise, add it
	* persist the new tag information into tlog
	* update tag inverted index

* add new attributes
    * obtain the LeafMNode of that timeseries
	* read attribute information through the offset in LeafMNode
	* iterate the attributes needed to be added, if it has existed, then throw exception, otherwise, add it
	* persist the new attribute information into tlog

* upsert tags/attributes
	* obtain the LeafMNode of that timeseries
	* read tag information through the offset in LeafMNode
	* iterate the tags and attributes needed to be upserted, if it has existed，use the new value to update it, otherwise, add it
	* persist the updated tags and attributes information into tlog
	* if the upserted ones include tag, we still need to update tag inverted index



## MTree

* org.apache.iotdb.db.metadata.MTree

There three types of nodes in MTree: StorageGroupMNode、InternalMNode(Non-leaf node)、LeafMNode(leaf node), they all extend to MNode.

Each InternalMNode has a read-write lock. When querying metadata information, you need to obtain a read lock for each InternalMNode on the path. When modifying metadata information, if you modify the LeafMNode, you need to obtain the write lock of its parent node. If you modify a non-leaf node, only need to obtain its own write lock. If the InternalMNode is located in the device layer, it also contains a `Map <String, MNode> aliasChildren`, which is used to store alias information.

StorageGroupMNode extends to InternalMNode, containing metadata information for storage groups, such as TTL.

LeafMNode contains the schema information of the corresponding time series, its alias(if it doesn't have, it is null) and the offset of the time series tag/attribute information in the tlog file(if there is no tag/attribute, it is -1)

example：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625246-fc3e8200-467e-11ea-8815-67b9c4ab716e.png">

The metadata management of IoTDB takes the form of a directory tree, the penultimate layer is the device layer, and the last layer is the sensor layer.

The root node exists by default. Creating storage groups, deleting storage groups, creating time series and deleting time series are all operations on the nodes of the tree.

* create storage group（root.a.b.sg）
	* create InternalMNode(a.b) for current storage group
	* make sure this prefix path doesn't contain any other storage group(storage group nesting is not allowed)
	* check if the storage group has existed
	* create StorageGroupMNode(sg)

* create timeseries（root.a.b.sg.d.s1）
	* walk the path and make sure the storage group has been created
	* find the node in the penultimate layer(device layer), check if it already has the child leaf node with same name
	* create LeafMNode, and store the alias in LeafMNode if it has
	* If it has alias, create another links with alias to LeafMNode

* Deleting a storage group is similar to deleting a time series. That is, the storage group or time series node is deleted in its parent node. The time series node also needs to delete its alias in the parent node; if in the deletion process, a node is found not to have any child node, needs to be deleted recursively.
	
## Log management of metadata

* org.apache.iotdb.db.metadata.MLogWriter

All metadata operations are recorded in a metadata log file, which defaults to data/system/schema/mlog.txt.

When the system restarted, the logs in mlog will be replayed. Until the replaying finished, you need to mark writeToLog to false. When the restart is complete, the writeToLog needs to be set to true.

The type of metadata log is recorded by the MetadataOperationType class. mlog directly stores the corresponding string encoding.

sql examples and the corresponding mlog record:

* set storage group to root.turbine

	> mlog: 2,root.turbine
	
	> format: 2,path

* delete storage group root.turbine	
	
	> mlog: 1,root.turbine
	
	> format: 1,path

* create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)

	> mlog: 0,root.turbine.d1.s1,3,2,1,,temperature,offset
	
	> format: 0,path,TSDataType,TSEncoding,CompressionType,[properties],[alias],[tag-attribute offset]

* delete timeseries root.turbine.d1.s1

	> mlog: 1,root.turbine.d1.s1
	
	> format: 1,path	

* set ttl to root.turbine 10
	
	> mlog: 10,root.turbine,10
		
	> format: 10,path,ttl

* alter timeseries root.turbine.d1.s1 add tags(tag1=v1)
   > Only when root.turbine.d1.s1 does not have any tag/attribute information before, the sql will generate logs
   
   > mlog: 12,root.turbine.d1.s1,10
   
   > format: 10,path,[change offset]

## TLog
* org.apache.iotdb.db.metadata.TagLogFile

All timeseries tag/attribute information will be saved in the tag file, which defaults to data/system/schema/mlog.txt.

* Total number of bytes of persistence for tags and attributes of each time series is L, which can be configured in the iotdb-engine.properties

* persist content: `Map<String,String> tags, Map<String,String> attributes`, if the content length doesn't reach L, we need to fill it with blank.

> create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes (attr1=v1, attr2=v2)

> content in tlog:

> tagsSize (tag1=v1, tag2=v2) attributesSize (attr1=v1, attr2=v2)

