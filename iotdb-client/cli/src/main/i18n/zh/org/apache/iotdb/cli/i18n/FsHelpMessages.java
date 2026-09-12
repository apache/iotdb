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

package org.apache.iotdb.cli.i18n;

public final class FsHelpMessages {
  private FsHelpMessages() {}

  public static final String GENERAL =
      "使用 help <command>、<command> -h 或 <command> --help 查看详细帮助。\n"
          + "包含空格的路径和模式需要加引号；以 - 开头的操作数前使用 --。\n"
          + "虚拟路径表示远端数据库对象；write/export/sketch 的本地路径相对于进程目录。\n"
          + "使用 | 连接管道；< 读取本地输入；> 和 >> 重定向到本地文件。\n"
          + "写操作需要 --fs_write_mode enabled。结果输出到 stdout，错误输出到 stderr。\n"
          + "状态码：0 成功，1 用法错误，2 输入错误，3 运行错误；grep 使用 0 匹配、1 无匹配、2 错误。";
  public static final String READ_OPTIONS =
      "-m 选择 FIELD 列，同时保留 time 和全部 TAG 列；-d 选择树模型设备，-t 选择表。\n"
          + "-n 限制数据行数，--offset 跳过匹配行；--start/--end 为包含端点的时间范围。\n"
          + "TAG 运算符：eq、neq、regexp、is-null、not-null。多个过滤条件必须指定 --tag-match all 或 any。\n"
          + "CSV 空值使用无引号的 \\N，字面值 \\N 加引号；NDJSON 保留 schema 类型并为 INT64/TIMESTAMP 加引号。";
  private static final String CURRENT = "默认当前虚拟目录；除已说明的结果外，成功时无额外输出。";
  private static final String INPUT = "处理全部输入；省略路径或使用 - 时读取 stdin。";
  private static final String WRITE = "必须指定路径，并启用 --fs_write_mode enabled。";
  public static final String[] PWD = {"输出当前虚拟目录的绝对路径。", "不接受选项。"};
  public static final String[] LS = {
    "列出虚拟对象；-a 包含 . 和 ..，-R 递归，-l 显示可用属性。不适用的 Unix 属性显示为 -。", CURRENT
  };
  public static final String[] LL = {"长格式列表，等同于 ls -l。", CURRENT};
  public static final String[] CD = {"切换虚拟目录；cd - 返回上一次目录。", "省略路径或使用 ~ 时进入虚拟主目录 /。"};
  public static final String[] STAT = {"输出虚拟对象类型、可读内容字节数和可用数据库元数据；不伪造 inode 或权限。", CURRENT};
  public static final String[] FILE = {"输出虚拟对象类型和可用内容描述。", CURRENT};
  public static final String[] SCHEMA = {
    "输出 model/object/column/category/data_type/encoding/compression；无法获得的物理属性为 NULL。",
    "默认当前范围的全部列；格式为 table、ndjson 或 csv。"
  };
  public static final String[] META = {"输出可用数据库对象元数据；描述远端对象，不代表本地 TsFile。", CURRENT};
  public static final String[] STATS = {
    "按设备输出类型相关的 FIELD 统计、TAG 值、真实空值数和非空时间范围；stats_source 为 scan。",
    "默认当前范围的全部 FIELD；BOOLEAN 的 sum 为 true 数量，INT64/TIMESTAMP/DATE 的 sum 为 NULL。"
  };
  public static final String[] COUNT = {
    "输出 TAG 和 FIELD 的行数、设备数、非空数、空值数及时间范围；排除 TIME 和 ATTRIBUTE。",
    "默认当前范围的全部 TAG/FIELD；树模型的 entity_count 为 NULL。"
  };
  public static final String[] CAT = {
    "输出所选的类型化数据行，或原样输出 .meta 文本。", "默认当前范围的全部匹配行；格式默认为 table，CSV 包含 schema 表头。"
  };
  public static final String[] HEAD = {"输出前若干匹配数据行，或输入文本的前若干行。", "默认当前范围的 10 行数据；数据表头不计入行数限制。"};
  public static final String[] TAIL = {
    "输出末尾若干行或字节；+N 从位置 N 开始，-f 持续读取新增内容；数据格式使用 --format。", "无路径时读取 stdin；默认 10 行。虚拟数据按 CSV 文本序列化。"
  };
  public static final String[] WC = {"统计可读内容的 UTF-8 字节数；多个输入额外输出总计；仅支持 -c。", INPUT};
  public static final String[] GREP = {
    "使用基本正则表达式；-E 使用扩展表达式，-F 匹配字面文本；支持忽略大小写、反选和行号。", "处理全部输入，无路径时读取 stdin；状态码为 0 匹配、1 无匹配、2 错误。"
  };
  public static final String[] FIND = {"递归输出名称匹配 shell 模式的路径，可限制对象类型和深度。", "默认当前目录、全部名称和无限深度。"};
  public static final String[] PAGING = {"终端中提供交互分页和搜索；批处理模式输出全部内容。", CURRENT};
  public static final String[] MKDIR = {"创建表模型数据库；-p 允许父目录已存在；虚拟对象不支持 Unix 权限模式 -m。", WRITE};
  public static final String[] RMDIR = {"删除空数据库；非空数据库会被拒绝。", WRITE};
  public static final String[] RM = {"删除表；-r 删除数据库及其内容，-f 忽略不存在的目标，-i 在删除前确认。", WRITE};
  public static final String[] MV = {"移动表，支持跨数据库；多个源要求目标为数据库；-n 跳过已存在目标，-i 在覆盖前确认。", WRITE};
  public static final String[] CP = {"复制表结构及数据，支持跨数据库；多个源要求目标为数据库；-n 跳过已存在目标，-i 在覆盖前确认。", WRITE};
  public static final String[] CUT = {
    "选择字段、字节或字符并保留空字段；列表支持 N、N-M、N-、-M；-s 忽略不含字段分隔符的行。", INPUT + " 字段分隔符默认为 TAB。"
  };
  public static final String[] PASTE = {"合并对应行；-s 串行合并各输入，-d 指定循环分隔符。", INPUT + " 分隔符默认为 TAB。"};
  public static final String[] JOIN = {
    "按键连接已排序输入；-a 包含未匹配行，-v 仅输出未匹配行，-e 替代空字段，-o 选择输出字段。", "必须提供两个输入；- 表示 stdin；默认第 1 字段为键并以空白分隔。"
  };
  public static final String[] TEE = {
    "读取 stdin 至 EOF，回显到 stdout 并将 CSV 数据写入目标表；-a 追加，否则替换现有表数据。",
    "无目标时仅回显 stdin；写入目标需要 --fs_write_mode enabled。"
  };
  public static final String[] EXPORT = {
    "按类型化数据格式将所选远端设备或表导出到本地文件。",
    "必须指定 --type 和对象；单对象用 -o，多对象用新的 --output-dir 并生成完成清单；--force 仅适用于 -o。"
  };
  public static final String[] SKETCH = {
    "检查本地 TsFile，在 sketch 标记间输出文件路径和模型。", "必须提供本地输入；默认输出到 stdout，-o 写入新文件，--force 允许覆盖，但不能覆盖输入自身。"
  };
  public static final String[] TREE = {"输出虚拟目录树。", "默认当前目录和无限深度；-L 0 不输出子对象。"};
  public static final String[] SQL = {
    "执行 SQL 语句并输出返回的结果行。", "保留语句原有引号；已识别只读查询以外的语句需要 --fs_write_mode enabled。"
  };
  public static final String[] HELP = {"输出通用帮助或指定命令帮助；-h 与 --help 等价。", "未指定命令时输出通用帮助。"};
  public static final String[] EXIT = {"按指定状态码退出文件系统模式；quit 为别名。", "状态码默认为 0；指定值保留低 8 位。"};
}
