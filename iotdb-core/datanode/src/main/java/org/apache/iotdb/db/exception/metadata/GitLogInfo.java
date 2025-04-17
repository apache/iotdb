package org.apache.iotdb.db.exception.metadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GitLogInfo {
  public static void main(String[] args) throws IOException {
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

    for (final String form : formal) {
      Process listFilesProcess =
          Runtime.getRuntime()
              .exec(
                  "git log --author=\""
                      + form
                      + "\" --pretty=tformat: --numstat | awk '{ add += $1; subs += $2; loc += $1 + $2 } END { printf \"added lines: %s, removed lines: %s, total lines: %s\\n\", add, subs, loc}'",
                  null,
                  new File("C:\\Users\\13361\\Documents\\GitHub\\iotdb\\"));
      BufferedReader listFilesReader =
          new BufferedReader(new InputStreamReader(listFilesProcess.getInputStream()));
      String line;
      while ((line = listFilesReader.readLine()) != null) {
        System.out.println(line);
      }
    }
  }
}
