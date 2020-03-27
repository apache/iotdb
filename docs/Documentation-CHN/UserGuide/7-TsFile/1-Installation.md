# 安装

开始之前，应先安装maven。 看到<a href="https://maven.apache.org/install.html">如何安装Maven</a>

在您自己的项目中有两种使用TsFile的方法。

* 用作 jars:
  * 编译源代码并构建为 jars

  	```
  	git clone https://github.com/apache/incubator-iotdb.git
  	cd tsfile/
  	sh package.sh
  	```
  	然后，所有jars都可以放在名为 `lib/`. 导入`lib/tsfile-0.8.2-jar-with-dependencies.jar` 到您的项目。

* 用作Maven依赖项：

  编译源代码并通过三个步骤将其部署到本地存储库：

  * 获取源代码

  	```
  	git clone https://github.com/apache/incubator-iotdb.git
  	```
  * 编译源代码并部署
  	
  	```
  	cd tsfile/
  	mvn clean install -Dmaven.test.skip=true
  	```
  * 在项目中添加依赖项：

    ```
  	 <dependency>
  	   <groupId>org.apache.iotdb</groupId>
  	   <artifactId>tsfile</artifactId>
  	   <version>0.8.2</version>
  	 </dependency>
    ```
    

  或者，您可以从官方Maven存储库下载依赖项：

  * 第一,找你的maven `settings.xml`在路径上： `${username}\.m2\settings.xml`
    , 加上这个 `<profile>` 至`<profiles>`:

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
  * 然后将依赖项添加到您的项目中：

    ```
     <dependency>
       <groupId>org.apache.iotdb</groupId>
       <artifactId>tsfile</artifactId>
       <version>0.8.2</version>
     </dependency>
    ```