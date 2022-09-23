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

# LSM Document
<pre>
  _____      ______   ____    ____  
 |_   _|   .' ____ \ |_   \  /   _| 
   | |     | (___ \_|  |   \/   |   
   | |   _  _.____`.   | |\  /| |   
  _| |__/ || \____) | _| |_\/_| |_  
 |________| \______.'|_____||_____| > version 0.14.0-SNAPSHOT
                                    
</pre>
## Abstract

The lsm framework has implemented the **memory structure** at present, and users only need to define the memory structure and access method of each level.


## Example
Suppose we need to implement an lsm storage engine to store each record similar to <tagKey, tagValue, id>, we define the following four layers of memory nodes.
### Memory structure
Implement the memory structure of each layer

- MemChunk

The last layer of memory nodes, save the id list
```java
public class MemChunk {
	List<Integer> deviceIDS;

	public MemChunk() {
		deviceIDS = new ArrayList<>();
	}

	public List<Integer> getDeviceIDS() {
		return deviceIDS;
	}

	public void setDeviceIDS(List<Integer> deviceIDS) {
		this.deviceIDS = deviceIDS;
	}

	@Override
	public String toString() {
		return "MemChunk{" + deviceIDS.toString() + '}';
	}
}
```

- MemGroup

Use a map to manage tagValue->MemChunk
```java
public class MemGroup {

	Map<String, MemChunk> map;

	public MemGroup() {
		map = new HashMap<>();
	}

	public Map<String, MemChunk> getMap() {
		return map;
	}

	public void setMap(Map<String, MemChunk> map) {
		this.map = map;
	}

	@Override
	public String toString() {
		return "MemGroup{" + map.toString() + '}';
	}
}
```

- MemTable

Use a map to manage tagKey->MemGroup
```java
public class MemTable {

	private Map<String, MemGroup> map;

	public MemTable() {
		map = new HashMap<>();
	}

	public Map<String, MemGroup> getMap() {
		return map;
	}

	public void setMap(Map<String, MemGroup> map) {
		this.map = map;
	}

	@Override
	public String toString() {
		return "MemTable{" + map.toString() + '}';
	}
}
```

- MemTableManager

Manage working memTable, and immutable memTable
```java
public class MemTableManager {

	private MemTable working;

	private Map<Integer, MemTable> immutables;

	private int maxDeviceID;

	public MemTableManager() {
		working = new MemTable();
		immutables = new HashMap<>();
		maxDeviceID = 0;
	}

	public MemTable getWorking() {
		return working;
	}

	public void setWorking(MemTable working) {
		this.working = working;
	}

	public Map<Integer, MemTable> getImmutables() {
		return immutables;
	}

	public void setImmutables(Map<Integer, MemTable> immutables) {
		this.immutables = immutables;
	}

	public int getMaxDeviceID() {
		return maxDeviceID;
	}

	public void setMaxDeviceID(int maxDeviceID) {
		this.maxDeviceID = maxDeviceID;
	}

	@Override
	public String toString() {
		return "MemTableManager{"
				+ "working="
				+ working.toString()
				+ ", immutables="
				+ immutables.toString()
				+ '}';
	}
}
```

### Access method
Incoming access to each layer for the framework

- Insertion and flush example
```java
public class Main {
	public static void main(String[] args) throws Exception {
		MemTableManager memTableManager = new MemTableManager();
		System.out.println("-------------insert--------------");
		insertionExample(memTableManager);
		System.out.println("-------------flush--------------");
		flushExample(memTableManager);
	}
    
	public static void insertionExample(MemTableManager memTableManager) throws Exception {
        // Initialize a BasicLsmManager to manage insert operations
		BasicLsmManager<MemTableManager, InsertRequestContext> baseLsmManager =
				new BasicLsmManager<MemTableManager, InsertRequestContext>();
		baseLsmManager
				.nextLevel(
                        // The insert method of the MemTableManager level
                        new InsertLevelProcess<MemTableManager, MemTable>() {
							@Override
							public List<MemTable> getChildren(
									MemTableManager memNode, InsertRequestContext context) {
								Integer deviceID = (Integer) context.getValue();
								int maxDeviceID = memNode.getMaxDeviceID();
								List<MemTable> children = new ArrayList<>();
								if (deviceID / 65536 == maxDeviceID / 65536) {
									children.add(memNode.getWorking());
								} else {
									children.add(memNode.getImmutables().get(deviceID / 65536));
								}
								return children;
							}

							@Override
							public void insert(MemTableManager memNode, InsertRequestContext context) {
								Integer deviceID = (Integer) context.getValue();
								int maxDeviceID = memNode.getMaxDeviceID();
								if (deviceID / 65536 == maxDeviceID / 65536) {
									if (memNode.getWorking() == null) {
										memNode.setWorking(new MemTable());
									}
								} else if (deviceID > maxDeviceID) {
									memNode
											.getImmutables()
											.put(memNode.getMaxDeviceID() / 65536, memNode.getWorking());
									memNode.setWorking(new MemTable());
								}
								if (deviceID > maxDeviceID) {
									memNode.setMaxDeviceID(deviceID);
								}
							}
						})
				.nextLevel(
                        // The insert method of the MemTable level
						new InsertLevelProcess<MemTable, MemGroup>() {
							@Override
							public List<MemGroup> getChildren(MemTable memNode, InsertRequestContext context) {
								String key = (String) context.getKey();
								List<MemGroup> children = new ArrayList<>();
								children.add(memNode.getMap().get(key));
								return children;
							}

							@Override
							public void insert(MemTable memNode, InsertRequestContext context) {
								String key = (String) context.getKey();
								Map<String, MemGroup> map = memNode.getMap();
								if (map.containsKey(key)) return;
								map.put(key, new MemGroup());
							}
						})
				.nextLevel(
                        // The insert method of the MemGroup level
						new InsertLevelProcess<MemGroup, MemChunk>() {
							@Override
							public List<MemChunk> getChildren(MemGroup memNode, InsertRequestContext context) {
								String key = (String) context.getKey();
								List<MemChunk> children = new ArrayList<>();
								children.add(memNode.getMap().get(key));
								return children;
							}

							@Override
							public void insert(MemGroup memNode, InsertRequestContext context) {
								String key = (String) context.getKey();
								Map<String, MemChunk> map = memNode.getMap();
								if (map.containsKey(key)) return;
								map.put(key, new MemChunk());
							}
						})
				.nextLevel(
                        // The insert method of the MemChunk level
						new InsertLevelProcess<MemChunk, Object>() {
							@Override
							public List<Object> getChildren(MemChunk memNode, InsertRequestContext context) {
								return null;
							}

							@Override
							public void insert(MemChunk memNode, InsertRequestContext context) {
								Integer deviceID = (Integer) context.getValue();
								List<Integer> deviceIDs = memNode.getDeviceIDS();
								deviceIDs.add(deviceID);
							}
						});
        
        // Insert some <tagKeytagValue,id> records
        // The key at the MemTableManager level is null
		baseLsmManager.process(memTableManager, new InsertRequestContext(1, null, "a", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(2, null, "a", "d"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(3, null, "a", "e"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(4, null, "a", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(5, null, "a1", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(6, null, "a2", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(65535, null, "a", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(65536, null, "a", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(2, null, "a", "d"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(3, null, "a", "e"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(4, null, "a", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(5, null, "a1", "b"));
		baseLsmManager.process(memTableManager, new InsertRequestContext(6, null, "a2", "b"));
        // process memTableManager
		System.out.println(memTableManager);
	}

	public static void flushExample(MemTableManager memTableManager) throws Exception {
        // Initialize a BasicLsmManager to manage insert operations
		BasicLsmManager<MemTableManager, FlushRequestContext> flushManager =
				new BasicLsmManager<MemTableManager, FlushRequestContext>();

		flushManager
				.nextLevel(
                        // The insert method of the MemTableManager level
						new FlushLevelProcess<MemTableManager, MemTable>() {
							@Override
							public void flush(MemTableManager memNode, FlushRequestContext context) {
								System.out.println("FLUSH: " + memNode + "-->[level:" + context.getLevel() + "]");
							}

							@Override
							public List<MemTable> getChildren(
									MemTableManager memNode, FlushRequestContext context) {
								List<MemTable> memTables = new ArrayList<>();
								memTables.addAll(memNode.getImmutables().values());
								if (memNode.getWorking() != null) memTables.add(memNode.getWorking());
								return memTables;
							}
						})
				.nextLevel(
                        // The insert method of the MemTable level
						new FlushLevelProcess<MemTable, MemGroup>() {
							@Override
							public void flush(MemTable memNode, FlushRequestContext context) {
								System.out.println("FLUSH: " + memNode + "-->[level:" + context.getLevel() + "]");
							}

							@Override
							public List<MemGroup> getChildren(MemTable memNode, FlushRequestContext context) {
								List<MemGroup> memGroups = new ArrayList<>();
								memGroups.addAll(memNode.getMap().values());
								return memGroups;
							}
						})
				.nextLevel(
                        // The insert method of the MemGroup level
						new FlushLevelProcess<MemGroup, MemChunk>() {
							@Override
							public void flush(MemGroup memNode, FlushRequestContext context) {
								System.out.println("FLUSH: " + memNode + "-->[level:" + context.getLevel() + "]");
							}

							@Override
							public List<MemChunk> getChildren(MemGroup memNode, FlushRequestContext context) {
								List<MemChunk> memChunk = new ArrayList<>();
								memChunk.addAll(memNode.getMap().values());
								return memChunk;
							}
						})
				.nextLevel(
                        // The insert method of the MemChunk level
						new FlushLevelProcess<MemChunk, Object>() {
							@Override
							public void flush(MemChunk memNode, FlushRequestContext context) {
								System.out.println("FLUSH: " + memNode + "-->[level:" + context.getLevel() + "]");
							}

							@Override
							public List<Object> getChildren(MemChunk memNode, FlushRequestContext context) {
								return new ArrayList<>();
							}
						});
        
        // process memTableManager
		flushManager.process(memTableManager, new FlushRequestContext());
	}
}

```
- Output

```txt
-------------insert--------------
MemTableManager{working=MemTable{{a=MemGroup{{b=MemChunk{[65536]}}}}}, immutables={0=MemTable{{a1=MemGroup{{b=MemChunk{[5, 5]}}}, a=MemGroup{{b=MemChunk{[1, 4, 65535, 4]}, d=MemChunk{[2, 2]}, e=MemChunk{[3, 3]}}}, a2=MemGroup{{b=MemChunk{[6, 6]}}}}}}}
-------------flush--------------
FLUSH: MemChunk{[5, 5]}-->[level:3]
FLUSH: MemChunk{[1, 4, 65535, 4]}-->[level:3]
FLUSH: MemChunk{[2, 2]}-->[level:3]
FLUSH: MemChunk{[3, 3]}-->[level:3]
FLUSH: MemChunk{[6, 6]}-->[level:3]
FLUSH: MemChunk{[65536]}-->[level:3]
FLUSH: MemGroup{{b=MemChunk{[5, 5]}}}-->[level:2]
FLUSH: MemGroup{{b=MemChunk{[1, 4, 65535, 4]}, d=MemChunk{[2, 2]}, e=MemChunk{[3, 3]}}}-->[level:2]
FLUSH: MemGroup{{b=MemChunk{[6, 6]}}}-->[level:2]
FLUSH: MemGroup{{b=MemChunk{[65536]}}}-->[level:2]
FLUSH: MemTable{{a1=MemGroup{{b=MemChunk{[5, 5]}}}, a=MemGroup{{b=MemChunk{[1, 4, 65535, 4]}, d=MemChunk{[2, 2]}, e=MemChunk{[3, 3]}}}, a2=MemGroup{{b=MemChunk{[6, 6]}}}}}-->[level:1]
FLUSH: MemTable{{a=MemGroup{{b=MemChunk{[65536]}}}}}-->[level:1]
FLUSH: MemTableManager{working=MemTable{{a=MemGroup{{b=MemChunk{[65536]}}}}}, immutables={0=MemTable{{a1=MemGroup{{b=MemChunk{[5, 5]}}}, a=MemGroup{{b=MemChunk{[1, 4, 65535, 4]}, d=MemChunk{[2, 2]}, e=MemChunk{[3, 3]}}}, a2=MemGroup{{b=MemChunk{[6, 6]}}}}}}}-->[level:0]
```
