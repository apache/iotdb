## TsFile Settle Tool

### Introduction

1. The settle tool helps you sort sealed TsFiles and mods files, filter out deleted data and generate new tsfiles.  If all the data in a TsFile is deleted, then the local tsFile and its corresponding resource file will be deleted.  
2. The TsFile settle tool can be used only for IOTDB v0.12, that is, it can only be used for TsFile which version is 3. If the version is later than this version, you must upgrade TsFile to V3 using the online upgrade tool first . 
3. TsFile settle tool can be divided into online settle tool and offline settle tool. Both of them record a settle log ("data\system\settle\settle.txt ") during the settling process, which can be used to recover the failed files next time.  If there are any failed files in the log, they will be settled first.  

### Offline Settle Tool

The offline settle tool is started in the command line, which startup scripts "settle.bat" and "settle.sh" are generated in the "server\target\iotdb-server-{version}\tools\tsfileToolSet" directory after server is compiled. When using the offline settle tool, ensure that the IOTDB server stops running; otherwise, an error may occur. See the UserGuide for more details.

#### 1.1 涉及的相关类

整理工具类：org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool

### 2.在线整理工具

在线整理工具是当用户在IOTDB客户端输入了settle命令后，会在后台注册启动一个整理服务Settle Service，该服务会去寻找指定存储组下的所有TsFile文件，并为每个TsFile开启一个整理线程进行整理。下面讲解整理线程的工作流程。

#### 2.1 整理线程的工作流程

1. 首先对该旧TsFile文件**加读锁**，避免在重写的过程中该TsFile被删除，而在删除该TsFile的本地mods文件前允许用户对该TsFile进行查询操作。【该步骤在StorageGroupProcessor类的settle()方法里】

2. 往整理日志记录标记该文件的状态为1。

3. 对旧TsFile和其mods文件进行整理并重写到新TsFile文件（此时新TsFile和对应的新.resource文件已经生成并存放在临时目录下，位置为：“旧TsFile所在目录\新Tsfile时间分区目录\”）。若该旧TsFile数据都已被删除了，则会直接删除本地TsFile及其.resource文件。【该步骤在TsFileAndModSettleTool类的settleOneTsFileAndMod()方法里】

4. 往整理日志记录标记该文件的状态为2。

5. 对旧TsFile文件**加写锁**，阻塞用户的查询等操作。【该步骤在StorageGroupProcessor类的settleTsFileCallBack()方法里】

6. 删除旧mods文件，然后将新TsFile和对应的新.resource文件移动到正确的目录下（即旧TsFile所在目录），此过程是先删除旧对应旧文件，再把新的移动过去。【该步骤在TsFileRewriteTool类的moveNewTsFile()方法里】

7. 清空内存中与该TsFile的相关缓存数据，即该被重写的TsFile在内存中涉及到的相关数据都应该被及时更新：

   (1) 需要clear cache (包括chunk cache和timeseriesmetadata cache)，防止因为Mods被删除而读出该TsFile已被删除的数据（脏数据）

   (2) 若整理后，此TsFile被删除，则要从StorageGroupProcessor的TsFileManagement里挪去该oldTsFileResource.

   【该步骤在StorageGroupProcessor类的settleTsFileCallBack()方法里】

8. 对该TsFile**释放写锁**，允许用户对该文件进行查询操作，然后**释放读锁**，允许该文件被删除。
9. 往整理日志标记该文件的状态为3，settle结束！

#### 2.2 注意事项

​	在线整理工具在整理过程中阻塞了用户对该虚拟存储组下的数据有任何的删除操作，直到整理完毕。

#### 2.3 补充说明

1. OldTsFileResource对应的本地TsFile文件不存在的情况

   (1) 上一次settle重写的过程中，旧TsFile因数据都被删掉了而相关文件被删掉，还没完成settle过程就出现异常导致整理线程中断。于是下次启动后发现settle.txt日志里的该TsFile整理记录对应的本地文件不存在。

   (2) 上次settle重写过程中，重写完后在移动的过程中，旧TsFile被删除而新TsFile还未移动前该线程就意外中断了。不过目前没有考虑处理这种情况。

2. Settle重写NewTsFileResource的返回情况（即TsFileAndModSettleTool类的settleOneTsFileAndMod()方法的返回情况）

   (1) 若旧TsFile没有封口，则不进行重写，返回null。

   (2) 若旧TsFile没有mods文件，即没有删除记录，则不进行重写，返回null。

   (3) 进行了重写后，旧TsFile的所有数据都被删除了，就会删除本地该TsFile文件和.resource文件，并且不会生成新的TsFile，则返回null。

#### 2.4 涉及的相关类

1. 整理服务类：org.apache.iotdb.db.service.SettleService
2. 整理线程类：org.apache.iotdb.db.engine.settle.SettleTask
3. 整理日志类：org.apache.iotdb.db.engine.settle.SettleLog
4. 整理工具类：org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool

#### 2.5 整理日志

1. 整理日志文件的路径在"data\system\settle\settle.txt"，它有一条条记录项，存放了某个TsFile的整理状态，每条记录项的格式为：<TsFile文件路径>,<整理状态>

2. 整理共有三个状态，每个状态代表的含义如下：

   (1) 状态1：代表该TsFile在整理中，还未生成新的TsFIle

   (2) 状态2：代表该TsFile的新TsFile已经生成，即整理线程已经把数据整理到新TsFile里了，还未移动到正确的目录下。

   (3) 状态3：代表该TsFile已经整理完毕且整理成功，旧TsFile对应的.mods文件已被删除，新的TsFile和对应的.resource文件已经移动到正确的目录下。

3. 当此次整理服务里所有的TsFile都被整理完毕且整理成功，就会删除整理日志settle.txt文件。

4. 整理日志可用于在下次重启时恢复上次整理失败的那些TsFile文件，具体的步骤为：

   1) 整理服务SettleService启动时，会先去查找settle.txt文件是否存在，若存在则读取日志中记录的那些整理失败的TsFile，把他们加入整理列表里。

   2) 开始整理：

   ​	(1) 对于状态是1的文件，则会删除上次整理失败而残留的新TsFile文件，并进行重写整理写入到新TsFile，然后删除对应的旧mods文件，并把新的TsFile和.resource文件移动到正确的目录下。

   ​	(2) 对于状态是2的文件，说明其新TsFile和对应的.resource文件已经生成，则只要把它们移动到正确的目录下即可。