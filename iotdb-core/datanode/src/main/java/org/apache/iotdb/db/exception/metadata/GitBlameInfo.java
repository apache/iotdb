package org.apache.iotdb.db.exception.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GitBlameInfo {
  public static void main(String[] args) throws IOException {
    final String timechoDBPath = "C:\\Users\\13361\\Documents\\GitHub\\timechodb\\";
    final String tsFilePath = "C:\\Users\\13361\\Documents\\GitHub\\tsfile\\";

    final Map<String, List<String>> inclusionPathMap = new HashMap<>();
    inclusionPathMap.put("IoTDB总", Collections.singletonList(""));
    inclusionPathMap.put("TsFile总", Collections.singletonList(""));

    inclusionPathMap.put(
        "流处理插件",
        Arrays.asList(
            "iotdb-api\\pipe-api\\src\\main\\java\\org\\apache\\iotdb\\pipe",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\pipe\\processor",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\pipe\\connector",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\pipe\\extractor"));
    inclusionPathMap.put(
        "元数据管理",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\schemaengine",
            "iotdb-protocol\\thrift-influxdb",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\exception",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\influxdb"));
    inclusionPathMap.put(
        "数据订阅",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\subscription"));
    inclusionPathMap.put(
        "流处理框架",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\pipe\\agent"));
    inclusionPathMap.put(
        "数据同步",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\pipe"));
    inclusionPathMap.put(
        "权限管理",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\auth",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\manager\\PermissionManager.java",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\persistence\\AuthorInfo.java",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\consensus\\request\\read\\auth",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\consensus\\request\\write\\auth",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\consensus\\response\\auth"));
    inclusionPathMap.put(
        "命令行工具",
        Arrays.asList(
            "iotdb-client\\cli\\src\\main",
            "iotdb-client\\cli\\src\\assembly",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\tools"));
    inclusionPathMap.put(
        "Java原生接口",
        Arrays.asList(
            "iotdb-client\\isession\\src\\main",
            "iotdb-client\\service-rpc\\src\\main",
            "iotdb-client\\session\\src\\main",
            "timecho-isession\\src\\main",
            "timecho-session\\src\\main"));
    inclusionPathMap.put("JDBC", Collections.singletonList("iotdb-client\\jdbc\\src\\main"));
    inclusionPathMap.put(
        "负载均衡管理",
        Arrays.asList(
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\manager\\node",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\manager\\partition",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\manager\\load"));
    inclusionPathMap.put(
        "RESTAPI",
        Arrays.asList(
            "iotdb-protocol\\openapi",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\rest"));
    inclusionPathMap.put(
        "共识层管理",
        Arrays.asList(
            "iotdb-core\\consensus\\src\\main",
            "iotdb-protocol\\thrift-consensus",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\consensus"));
    inclusionPathMap.put("授权激活", Collections.singletonList("timecho-confignode\\src"));
    inclusionPathMap.put(
        "加密通信",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\service\\RPCService.java",
            "iotdb-core\\node-commons\\src\\main\\java\\org\\apache\\iotdb\\commons\\service\\AbstractThriftServiceThread.java",
            "iotdb-core\\node-commons\\src\\main\\java\\org\\apache\\iotdb\\commons\\service\\ThriftServiceThread.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\thrift"));
    inclusionPathMap.put("白名单管理", Collections.singletonList("timecho-server\\src\\main"));
    inclusionPathMap.put(
        "安全审计",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\audit"));
    inclusionPathMap.put(
        "身份鉴别",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\auth\\BasicAuthorityCache.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\session"));
    inclusionPathMap.put(
        "配置管理",
        Arrays.asList(
            "iotdb-core\\node-commons\\src\\main\\java\\org\\apache\\iotdb\\commons\\conf",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\conf",
            "iotdb-core\\confignode\\src\\main\\java\\org\\apache\\iotdb\\confignode\\conf",
            "iotdb-core\\confignode\\src\\assembly",
            "iotdb-core\\datanode\\src\\assembly"));
    inclusionPathMap.put(
        "监控框架",
        Arrays.asList(
            "iotdb-core\\metrics",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\metrics",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\service"));
    inclusionPathMap.put(
        "SQL解析",
        Arrays.asList(
            "iotdb-core\\antlr",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\statement",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\analyze",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\expression"));
    inclusionPathMap.put(
        "查询规划",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\planner",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\scheduler",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\execution",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\Coordinator.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\common",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\QueryIdGenerator.java",
            "iotdb-api\\udf-api"));
    inclusionPathMap.put(
        "查询优化",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\plan\\optimization"));
    inclusionPathMap.put(
        "查询执行",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\exception",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\metric",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\statistics",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\transformation",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\memory",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\aggregation",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\exchange",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\executor",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\fragment",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\operator",
            "iotdb-core\\datanode\\src\\main\\codegen",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\MemoryEstimationHelper.java"));
    inclusionPathMap.put(
        "查询调度",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\schedule",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\driver",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\FutureStateChange.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\QueryState.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\QueryStateMachine.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\queryengine\\execution\\StateMachine.java"));
    inclusionPathMap.put(
        "LSM文件整理",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\compaction"));
    inclusionPathMap.put(
        "写前日志",
        Collections.singletonList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\wal"));
    inclusionPathMap.put(
        "文件管理",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\tsfile",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\modification",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\read",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion\\snapshot",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\load",
            "object-storage\\src\\main"));
    inclusionPathMap.put(
        "内存缓冲区",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\utils",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\rescon",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\StorageEngine.java",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\buffer",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\storageengine\\dataregion"));
    inclusionPathMap.put(
        "集群任务管理",
        Arrays.asList(
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\basic",
            "iotdb-core\\datanode\\src\\main\\java\\org\\apache\\iotdb\\db\\protocol\\client",
            "iotdb-core\\node-commons\\src\\main",
            "iotdb-core\\node-commons\\src\\assembly",
            "iotdb-core\\confignode\\src\\main",
            "iotdb-protocol\\thrift-confignode"));
    inclusionPathMap.put(
        "TsFile文件格式",
        Arrays.asList(
            "java\\common\\src\\main\\java\\org\\apache\\tsfile",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\common",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\file",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\fileSystem",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\encoding",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\compress",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\read",
            "java\\tsfile\\src\\main\\java\\org\\apache\\tsfile\\write"));

    final Map<String, List<String>> exclusionPaths = new HashMap<>();

    // Distinct logic
    inclusionPathMap.forEach(
        (name1, pathList1) ->
            pathList1.forEach(
                path1 ->
                    inclusionPathMap.forEach(
                        (name2, pathList2) ->
                            pathList2.stream()
                                .filter(path2 -> path2.contains(path1) && !name1.equals(name2))
                                .forEach(
                                    path2 ->
                                        exclusionPaths
                                            .computeIfAbsent(name1, k -> new ArrayList<>())
                                            .add(path2.substring(path1.length() + 1))))));

    final Set<String> formal =
        new HashSet<>(
            Arrays.asList(
                "Jackie Tien",
                "Caideyipi",
                "shuwenwei",
                "Steve Yurong Su",
                "Haonan",
                "Beyyes",
                "Weihao Li",
                "Zhijia Cao",
                "Li Yu Heng",
                "Potato",
                "Zhang.Jinrui",
                "Yuheng Li",
                "Jinrui Zhang",
                "Jialin Qiao",
                "CritasWang",
                "Gaofei Cao",
                "changxue2022",
                "曹志佳",
                "Yuan Tian",
                "cao zhijia",
                "hunter hou",
                "陈 哲涵",
                "旋 王",
                "chang xue",
                "shu wenwei",
                "JackieTien97",
                "Tan xinyu",
                "liyuheng",
                "HTHou",
                "张正明",
                "Jinrui.Zhang",
                "qiaojialin",
                "OneSizeFitQuorum",
                "Summer",
                "Steve Yurong Su (宇荣)",
                "YuFengLiu",
                "Xuan Ronaldo",
                "xiaoyekanren",
                "ppppoooo",
                "xingtanzjr",
                "CloudWise-Lukemiao",
                "yufeng liu",
                "Christofer Dutz"));
    final Set<String> internship =
        new HashSet<>(
            Arrays.asList(
                "Lin Xintao",
                "YongzaoDan",
                "Alima777",
                "Caiyin Yang",
                "ZhangHongYin",
                "JiaXin Zhang",
                "Jiang Tian",
                "Da Rui Lei",
                "MarcosZyk",
                "Marcos_Zyk",
                "CRZbulabula",
                "Liao Lanyu",
                "CRZbulabula",
                "Liao Lanyu",
                "江天",
                "张凌哲",
                "Itami Sho",
                "Peng Junzhi",
                "zyk990424",
                "Lei Rui",
                "lancelly",
                "WilliamSong11",
                "linxt20",
                "DESKTOP-L0L5GPJ\\jt",
                "YC27",
                "Rui,Lei",
                "V_Galaxy",
                "Colin Li",
                "YangCaiyin",
                "Zhenyu Luo",
                "Xiangwei Wei",
                "RuiLei",
                "yuyong",
                "jt",
                "chen yanze",
                "Zhihao Shen",
                "zhanglingzhe0820",
                "Mrquan",
                "SzyWilliam",
                "William Song",
                "Tian Jiang",
                "jt2594838",
                "Zikun Ma",
                "马子坤",
                "Xiangpeng Hu",
                "Chen YZ",
                "Yongzao",
                "ZhaoXin",
                "liuminghui233",
                "choubenson",
                "周沛辰"));
    formal.addAll(internship);

    for (final Map.Entry<String, List<String>> entry : inclusionPathMap.entrySet()) {
      System.out.println(entry.getKey());
      final File outPutFile =
          new File("C:\\Users\\13361\\Downloads\\output\\" + entry.getKey() + ".txt");
      outPutFile.delete();
      outPutFile.createNewFile();

      final ExecutorService executorService = Executors.newFixedThreadPool(14);
      int fileCount = 0;
      AtomicInteger token = new AtomicInteger(0);

      final AtomicInteger formalCode = new AtomicInteger(0);
      final AtomicInteger formalComment = new AtomicInteger(0);
      final AtomicInteger formalBlank = new AtomicInteger(0);

      final AtomicInteger employeeCode = new AtomicInteger(0);
      final AtomicInteger employeeComment = new AtomicInteger(0);
      final AtomicInteger employeeBlank = new AtomicInteger(0);

      final AtomicInteger internCode = new AtomicInteger(0);
      final AtomicInteger internComment = new AtomicInteger(0);
      final AtomicInteger internBlank = new AtomicInteger(0);

      final AtomicInteger outdatedCode = new AtomicInteger(0);
      final AtomicInteger outdatedComment = new AtomicInteger(0);
      final AtomicInteger outdatedBlank = new AtomicInteger(0);

      // Filter git hash
      final Pattern matcher = Pattern.compile("^[0-9a-z]{40}");
      final Set<String> keys =
          new HashSet<>(
              Arrays.asList(
                  "author ",
                  "author-mail ",
                  "author-time ",
                  "author-tz ",
                  "committer ",
                  "committer-mail ",
                  "committer-time ",
                  "committer-tz ",
                  "summary ",
                  "filename ",
                  "previous ",
                  "author-mail ",
                  "committer-date ",
                  "author-date ",
                  "body ",
                  "subject ",
                  "email ",
                  "score ",
                  "lines ",
                  "origin ",
                  "incomplete",
                  "boundary"));
      Map<String, Integer> codeLines = new ConcurrentHashMap<>();
      Map<String, Integer> commentLines = new ConcurrentHashMap<>();
      Map<String, Integer> blankLines = new ConcurrentHashMap<>();
      try {
        for (final String path : entry.getValue()) {
          final File file =
              new java.io.File(
                  (!(entry.getKey().equals("TsFile文件格式") || entry.getKey().equals("TsFile总"))
                          ? timechoDBPath
                          : tsFilePath)
                      + path);

          // 获取所有被 Git 追踪的文件
          Process listFilesProcess =
              file.isDirectory()
                  ? Runtime.getRuntime().exec("git ls-files", null, file)
                  : Runtime.getRuntime()
                      .exec("git ls-files " + file.getName(), null, file.getParentFile());

          BufferedReader listFilesReader =
              new BufferedReader(new InputStreamReader(listFilesProcess.getInputStream()));

          String filePath;
          while ((filePath = listFilesReader.readLine()) != null) {
            if (exclusionPaths.containsKey(entry.getKey())
                && exclusionPaths.get(entry.getKey()).stream().anyMatch(filePath::contains)) {
              continue;
            }
            // 对每个文件调用 git blame
            ++fileCount;
            final String currentFilePath = filePath;
            token.incrementAndGet();
            executorService.submit(
                () -> {
                  try {
                    final File currentDir =
                        new java.io.File(
                            (!(entry.getKey().equals("TsFile文件格式")
                                        || entry.getKey().equals("TsFile总"))
                                    ? timechoDBPath
                                    : tsFilePath)
                                + path);
                    if (((entry.getKey().equals("RESTAPI") || entry.getKey().equals("授权激活"))
                            && (currentFilePath.contains(".xml")
                                || currentFilePath.contains(".yaml")))
                        || (entry.getKey().equals("白名单管理")
                            && (currentFilePath.contains("white.list")
                                || currentFilePath.contains("META-INF")))
                        || (entry.getKey().equals("配置管理")
                            && (currentFilePath.contains(".xml")
                                || currentFilePath.contains(".sh")
                                || currentFilePath.contains(".bat")
                                || currentFilePath.contains(".access")
                                || currentFilePath.contains(".password")))) {
                      token.decrementAndGet();
                      return;
                    }
                    Process blameProcess =
                        Runtime.getRuntime()
                            .exec(
                                "git blame --line-porcelain " + currentFilePath,
                                null,
                                currentDir.isDirectory() ? currentDir : currentDir.getParentFile());
                    BufferedReader blameReader =
                        new BufferedReader(new InputStreamReader(blameProcess.getInputStream()));

                    String line;
                    String author = null;
                    boolean commentBlock = false;
                    boolean isFormal = false;
                    boolean outdated = false;
                    while ((line = blameReader.readLine()) != null) {
                      boolean hitKey = false;
                      if (line.startsWith("author ")) {
                        author = line.substring(7);
                        isFormal = formal.contains(author);
                      } else if (line.startsWith("author-time ")) {
                        // Filter author-time earlier than 2021.10.20
                        if (Long.parseLong(line.split(" ")[1]) <= 1634659200) {
                          isFormal = false;
                          outdated = true;
                        } else {
                          outdated = false;
                        }
                      } else {
                        for (final String key : keys) {
                          if (line.startsWith(key)) {
                            hitKey = true;
                            break;
                          }
                        }
                        if (hitKey || matcher.matcher(line).find()) {
                          continue;
                        }
                        if (line.trim().isEmpty()) {
                          if (isFormal) {
                            formalBlank.incrementAndGet();
                            if (internship.contains(author)) {
                              internBlank.incrementAndGet();
                            } else {
                              employeeBlank.incrementAndGet();
                            }
                          } else if (outdated) {
                            outdatedCode.incrementAndGet();
                          }
                          blankLines.compute(author, (k, v) -> Objects.isNull(v) ? 1 : v + 1);
                        } else if (line.trim().startsWith("//")
                            || line.trim().startsWith("/*")
                            || commentBlock) {
                          if (line.trim().contains("/*")) {
                            commentBlock = true;
                          }
                          if (line.trim().contains("*/")) {
                            commentBlock = false;
                          }
                          if (isFormal) {
                            formalComment.incrementAndGet();
                            if (internship.contains(author)) {
                              internComment.incrementAndGet();
                            } else {
                              employeeComment.incrementAndGet();
                            }
                          } else if (outdated) {
                            outdatedCode.incrementAndGet();
                          }
                          commentLines.compute(author, (k, v) -> Objects.isNull(v) ? 1 : v + 1);
                        } else {
                          if (isFormal) {
                            formalCode.incrementAndGet();
                            if (internship.contains(author)) {
                              internCode.incrementAndGet();
                            } else {
                              employeeCode.incrementAndGet();
                            }
                          } else if (outdated) {
                            outdatedCode.incrementAndGet();
                          }
                          codeLines.compute(author, (k, v) -> Objects.isNull(v) ? 1 : v + 1);
                        }
                      }
                    }

                    // 等待 blame 进程结束
                    blameProcess.waitFor();
                    token.decrementAndGet();
                  } catch (final Exception e) {
                    System.out.println(e);
                  }
                });
          }
          listFilesProcess.waitFor();
        }

        try (FileWriter writer = new FileWriter(outPutFile)) {
          writeFile(writer, "File count: " + fileCount);

          while (token.get() != 0) {
            Thread.sleep(50);
          }
          // 输出结果
          writeFile(writer, "");

          writeFile(writer, "Code Lines: " + codeLines.values().stream().reduce(0, Integer::sum));
          getSortedMap(codeLines).forEach((k, v) -> writeFile(writer, k + ": " + v));

          writeFile(writer, "");
          writeFile(
              writer, "Comment Lines: " + commentLines.values().stream().reduce(0, Integer::sum));
          getSortedMap(commentLines).forEach((k, v) -> writeFile(writer, k + ": " + v));
          writeFile(writer, "");

          writeFile(writer, "Blank Lines: " + blankLines.values().stream().reduce(0, Integer::sum));
          getSortedMap(blankLines).forEach((k, v) -> writeFile(writer, k + ": " + v));
          writeFile(writer, "");

          writeFile(writer, "");

          final Map<String, Integer> totalMap = new HashMap<>();
          for (final String name : codeLines.keySet()) {
            totalMap.put(
                name,
                codeLines.getOrDefault(name, 0)
                    + commentLines.getOrDefault(name, 0)
                    + blankLines.getOrDefault(name, 0));
          }
          for (final String name : commentLines.keySet()) {
            if (!totalMap.containsKey(name)) {
              totalMap.put(
                  name,
                  codeLines.getOrDefault(name, 0)
                      + commentLines.getOrDefault(name, 0)
                      + blankLines.getOrDefault(name, 0));
            }
          }
          for (final String name : blankLines.keySet()) {
            if (!totalMap.containsKey(name)) {
              totalMap.put(
                  name,
                  codeLines.getOrDefault(name, 0)
                      + commentLines.getOrDefault(name, 0)
                      + blankLines.getOrDefault(name, 0));
            }
          }

          writeFile(writer, "Total Lines: " + totalMap.values().stream().reduce(0, Integer::sum));
          getSortedMap(totalMap).forEach((k, v) -> writeFile(writer, k + ": " + v));

          writeFile(writer, "");
          writeFile(writer, "Formal code: " + formalCode);
          writeFile(writer, "Formal comment: " + formalComment);
          writeFile(writer, "Formal blank: " + formalBlank);
          writeFile(
              writer,
              "Formal total: " + (formalCode.get() + formalComment.get() + formalBlank.get()));

          writeFile(writer, "");
          writeFile(writer, "Formal code: " + employeeCode);
          writeFile(writer, "Formal comment: " + employeeComment);
          writeFile(writer, "Formal blank: " + employeeBlank);
          writeFile(
              writer,
              "Formal total: " + (formalCode.get() + formalComment.get() + formalBlank.get()));

          writeFile(writer, "");
          writeFile(writer, "Formal code: " + internCode);
          writeFile(writer, "Formal comment: " + internComment);
          writeFile(writer, "Formal blank: " + internBlank);
          writeFile(
              writer,
              "Formal total: " + (formalCode.get() + formalComment.get() + formalBlank.get()));

          writeFile(writer, "");
          writeFile(writer, "Previous code: " + outdatedCode);
          writeFile(writer, "Previous comment: " + outdatedComment);
          writeFile(writer, "Previous blank: " + outdatedBlank);
          writeFile(
              writer,
              "Previous total: "
                  + (outdatedCode.get() + outdatedComment.get() + outdatedBlank.get()));
        }

        executorService.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void writeFile(final FileWriter writer, final String s) {
    // System.out.println(s);
    try {
      writer.write(s + "\n");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, Integer> getSortedMap(final Map<String, Integer> map) {
    return map.entrySet().stream()
        .sorted((c1, c2) -> c2.getValue().compareTo(c1.getValue()))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
  }
}
