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

# Chapter 7: TsFile

## Installation

Before started, maven should be installed. See <a href="https://maven.apache.org/install.html">How to install maven</a>

There are two ways to use TsFile in your own project.

* Using as jars:
	* Compile the source codes and build to jars
	
		```
		git clone https://github.com/apache/incubator-iotdb.git
		cd tsfile/
		sh package.sh
		```
		Then, all the jars can be get in folder named `lib/`. Import `lib/tsfile-0.8.2-jar-with-dependencies.jar` to your project.
	
* Using as a maven dependency: 

	Compile source codes and deploy to your local repository in three steps:

	* Get the source codes
	
		```
		git clone https://github.com/apache/incubator-iotdb.git
		```
	* Compile the source codes and deploy 
		
		```
		cd tsfile/
		mvn clean install -Dmaven.test.skip=true
		```
	* add dependencies into your project:
	
	  ```
		 <dependency>
		   <groupId>org.apache.iotdb</groupId>
		   <artifactId>tsfile</artifactId>
		   <version>0.8.2</version>
		 </dependency>
	  ```
	  
	Or, you can download the dependencies from official Maven repository:
	
	* First, find your maven `settings.xml` on path: `${username}\.m2\settings.xml`
	  , add this `<profile>` to `<profiles>`:
	  ```
	    <profile>
           <id>allow-snapshots</id>
              <activation><activeByDefault>true</activeByDefault></activation>
           <repositories>
             <repository>  
                <id>apache.snapshots</id>
                <name>Apache Development Snapshot Repository</name>
                <url>https://repository.apache.org/content/repositories/snapshots/</url>
                <releases>
                    <enabled>false</enabled>
                </releases>
                <snapshots>
                    <enabled>true</enabled>
                </snapshots>
              </repository>
           </repositories>
         </profile>
	  ```
	* Then add dependencies into your project:
	
	  ```
		 <dependency>
		   <groupId>org.apache.iotdb</groupId>
		   <artifactId>tsfile</artifactId>
		   <version>0.8.2</version>
		 </dependency>
	  ```