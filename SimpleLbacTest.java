import java.util.HashMap;
import java.util.Map;

// 导入核心LBAC类
import org.apache.iotdb.commons.schema.SecurityLabel;
import org.apache.iotdb.db.auth.LabelPolicyEvaluator;
import org.apache.iotdb.db.auth.DatabaseLabelFetcher;

/**
 * 简化的LBAC框架功能验证脚本
 * 专注测试核心算法和逻辑，避免复杂的IoTDB依赖
 */
public class SimpleLbacTest {

    private static int totalTests = 0;
    private static int passedTests = 0;

    public static void main(String[] args) {
        System.out.println("🔍 开始简化LBAC框架功能验证...\n");

        // 测试1：SecurityLabel基本功能
        System.out.println("=== 测试1：SecurityLabel基本功能 ===");
        testSecurityLabel();

        // 测试2：策略表达式解析 - 字符串比较
        System.out.println("\n=== 测试2：字符串比较 ===");
        testStringComparison();

        // 测试3：策略表达式解析 - 数值比较xxx
        System.out.println("\n=== 测试3：数值比较 ===");
        testNumericComparison();

        // 测试4：逻辑操作符
        System.out.println("\n=== 测试4：逻辑操作符 ===");
        testLogicalOperators();

        // 测试5：复杂表达式
        System.out.println("\n=== 测试5：复杂表达式 ===");
        testComplexExpressions();

        // 测试6：边界情况
        System.out.println("\n=== 测试6：边界情况 ===");
        testEdgeCases();

        // 测试7：数据库路径提取
        System.out.println("\n=== 测试7：数据库路径提取 ===");
        testDatabasePathExtraction();

        // 测试8：语法验证
        System.out.println("\n=== 测试8：语法验证 ===");
        testSyntaxValidation();

        // 测试9：实际场景
        System.out.println("\n=== 测试9：实际场景验证 ===");
        testRealWorldScenarios();

        // 测试10：性能测试
        System.out.println("\n=== 测试10：性能测试 ===");
        testPerformance();

        // 输出总结
        System.out.println("\n" + "=".repeat(50));
        System.out.println(String.format("🎉 测试完成！通过: %d/%d (%.1f%%)",
                passedTests, totalTests, (double)passedTests/totalTests*100));

        if (passedTests == totalTests) {
            System.out.println("✅ 所有测试都通过了！LBAC框架功能正常！");
        } else {
            System.out.println("❌ 有部分测试失败，请检查具体问题。");
        }
    }

    private static void testSecurityLabel() {
        try {
            // 基本功能测试
            SecurityLabel label = new SecurityLabel();
            assertTest(label.isEmpty(), "新建SecurityLabel应该为空");

            label.setLabel("env", "prod");
            assertTest(!label.isEmpty(), "添加标签后不应该为空");
            assertTest("prod".equals(label.getLabel("env")), "应该返回正确的标签值");
            assertTest(label.size() == 1, "应该有1个标签");

            label.setLabel("region", "cn");
            assertTest(label.size() == 2, "应该有2个标签");

            String removedValue = label.removeLabel("env");
            assertTest("prod".equals(removedValue), "移除的值应该正确");
            assertTest(label.size() == 1, "移除后应该有1个标签");

            // 键值测试
            label.setLabel("level", "3");
            assertTest(label.getLabel("level") != null, "应该包含level键");
            assertTest(label.getLabel("nonexistent") == null, "不应该包含不存在的键");

            System.out.println("  标签内容: " + label.toString());

        } catch (Exception e) {
            System.out.println("✗ SecurityLabel测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testStringComparison() {
        try {
            SecurityLabel testLabel = createTestLabel();

            // 字符串相等比较
            assertTest(LabelPolicyEvaluator.evaluate("env=\"prod\"", testLabel), "env='prod'应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("region=\"cn\"", testLabel), "region='cn'应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"test\"", testLabel), "env='test'不应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("region=\"us\"", testLabel), "region='us'不应该匹配");

            // 字符串不等比较
            assertTest(LabelPolicyEvaluator.evaluate("env!=\"test\"", testLabel), "env!='test'应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("region!=\"us\"", testLabel), "region!='us'应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env!=\"prod\"", testLabel), "env!='prod'不应该匹配");

        } catch (Exception e) {
            System.out.println("✗ 字符串比较测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testNumericComparison() {
        try {
            SecurityLabel testLabel = createTestLabel();

            // 数值相等比较
            assertTest(LabelPolicyEvaluator.evaluate("level=3", testLabel), "level=3应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("level=5", testLabel), "level=5不应该匹配");

            // 数值不等比较
            assertTest(LabelPolicyEvaluator.evaluate("level!=5", testLabel), "level!=5应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("level!=3", testLabel), "level!=3不应该匹配");

            // 数值大小比较
            assertTest(LabelPolicyEvaluator.evaluate("level>2", testLabel), "level>2应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("level>=3", testLabel), "level>=3应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("level<5", testLabel), "level<5应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("level<=3", testLabel), "level<=3应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("level>5", testLabel), "level>5不应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("level<2", testLabel), "level<2不应该匹配");

            // 数值边界测试
            assertTest(LabelPolicyEvaluator.evaluate("level>=0", testLabel), "level>=0应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("level<=10", testLabel), "level<=10应该匹配");

        } catch (Exception e) {
            System.out.println("✗ 数值比较测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testLogicalOperators() {
        try {
            SecurityLabel testLabel = createTestLabel();

            // AND操作符
            assertTest(LabelPolicyEvaluator.evaluate("env=\"prod\" AND region=\"cn\"", testLabel),
                    "env='prod' AND region='cn'应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"prod\" AND region=\"us\"", testLabel),
                    "env='prod' AND region='us'不应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"test\" AND region=\"cn\"", testLabel),
                    "env='test' AND region='cn'不应该匹配");

            // OR操作符
            assertTest(LabelPolicyEvaluator.evaluate("env=\"prod\" OR region=\"us\"", testLabel),
                    "env='prod' OR region='us'应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("env=\"test\" OR region=\"cn\"", testLabel),
                    "env='test' OR region='cn'应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"test\" OR region=\"us\"", testLabel),
                    "env='test' OR region='us'不应该匹配");

            // 混合逻辑操作符
            assertTest(LabelPolicyEvaluator.evaluate("env=\"prod\" AND region=\"cn\" OR role=\"admin\"", testLabel),
                    "复杂AND/OR表达式应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("env=\"test\" OR region=\"cn\" AND level=3", testLabel),
                    "复杂AND/OR表达式应该匹配");

        } catch (Exception e) {
            System.out.println("✗ 逻辑操作符测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testComplexExpressions() {
        try {
            SecurityLabel testLabel = createTestLabel();

            // 括号优先级
            assertTest(LabelPolicyEvaluator.evaluate("(env=\"prod\" OR env=\"test\") AND region=\"cn\"", testLabel),
                    "(env='prod' OR env='test') AND region='cn'应该匹配");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"test\" OR (env=\"prod\" AND region=\"us\")", testLabel),
                    "env='test' OR (env='prod' AND region='us')不应该匹配");

            // 复杂策略表达式
            String complexPolicy1 = "env=\"prod\" AND region=\"cn\" AND level>=2 AND role!=\"guest\"";
            assertTest(LabelPolicyEvaluator.evaluate(complexPolicy1, testLabel), "复杂策略1应该匹配");

            String complexPolicy2 = "(env=\"prod\" OR env=\"staging\") AND (region=\"cn\" OR region=\"us\") AND level<5";
            assertTest(LabelPolicyEvaluator.evaluate(complexPolicy2, testLabel), "复杂策略2应该匹配");

            String complexPolicy3 = "env!=\"test\" AND (region=\"cn\" OR (region=\"us\" AND level>5))";
            assertTest(LabelPolicyEvaluator.evaluate(complexPolicy3, testLabel), "复杂策略3应该匹配");

            // 嵌套括号
            assertTest(LabelPolicyEvaluator.evaluate("((env=\"prod\" AND region=\"cn\") OR (env=\"test\" AND region=\"us\")) AND level=3", testLabel),
                    "嵌套括号表达式应该正确计算");

        } catch (Exception e) {
            System.out.println("✗ 复杂表达式测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testEdgeCases() {
        try {
            SecurityLabel testLabel = createTestLabel();

            // 测试缺失标签
            assertTest(!LabelPolicyEvaluator.evaluate("nonexistent=\"value\"", testLabel),
                    "不存在的标签应该返回false");
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"prod\" AND nonexistent=\"value\"", testLabel),
                    "部分缺失标签的AND表达式应该返回false");

            // 测试空标签
            SecurityLabel emptyLabel = new SecurityLabel();
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"prod\"", emptyLabel),
                    "空标签对象应该匹配失败");

            // 测试null标签
            assertTest(!LabelPolicyEvaluator.evaluate("env=\"prod\"", null),
                    "null标签对象应该匹配失败");

            // 测试错误处理
            try {
                LabelPolicyEvaluator.evaluate(null, testLabel);
                assertTest(false, "null策略表达式应该抛出异常");
            } catch (IllegalArgumentException e) {
                assertTest(true, "null策略表达式正确抛出异常");
            }

            try {
                LabelPolicyEvaluator.evaluate("", testLabel);
                assertTest(false, "空策略表达式应该抛出异常");
            } catch (IllegalArgumentException e) {
                assertTest(true, "空策略表达式正确抛出异常");
            }

            // 测试特殊字符
            SecurityLabel specialLabel = new SecurityLabel();
            specialLabel.setLabel("path", "/root/test");
            specialLabel.setLabel("name", "test_file");
            assertTest(LabelPolicyEvaluator.evaluate("path=\"/root/test\"", specialLabel),
                    "特殊字符路径应该匹配");
            assertTest(LabelPolicyEvaluator.evaluate("name=\"test_file\"", specialLabel),
                    "下划线字符应该匹配");

        } catch (Exception e) {
            System.out.println("✗ 边界情况测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testDatabasePathExtraction() {
        try {
            String path1 = "root.test.device1.sensor1";
            String dbPath1 = DatabaseLabelFetcher.extractDatabasePath(path1);
            assertTest("root.test".equals(dbPath1), "应该正确提取数据库路径: " + dbPath1);

            String path2 = "root.production.factory1.line1.temperature";
            String dbPath2 = DatabaseLabelFetcher.extractDatabasePath(path2);
            assertTest("root.production".equals(dbPath2), "应该正确提取数据库路径: " + dbPath2);

            String path3 = "root.demo";
            String dbPath3 = DatabaseLabelFetcher.extractDatabasePath(path3);
            assertTest("root.demo".equals(dbPath3), "两层路径应该返回完整路径: " + dbPath3);

            String invalidPath = "invalid.path";
            String dbPath4 = DatabaseLabelFetcher.extractDatabasePath(invalidPath);
            assertTest(dbPath4 == null, "无效路径应该返回null");

            String rootPath = "root";
            String dbPath5 = DatabaseLabelFetcher.extractDatabasePath(rootPath);
            assertTest(dbPath5 == null, "单层root路径应该返回null");

            // 测试缓存功能
            int cacheSize = DatabaseLabelFetcher.getCacheSize();
            assertTest(cacheSize >= 0, "缓存大小应该非负");

            DatabaseLabelFetcher.clearCache();
            assertTest(DatabaseLabelFetcher.getCacheSize() == 0, "清理后缓存应该为空");

        } catch (Exception e) {
            System.out.println("✗ 数据库路径提取测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testSyntaxValidation() {
        try {
            // 有效表达式
            assertTest(LabelPolicyEvaluator.isValidExpression("env=\"prod\" AND region=\"cn\""),
                    "有效表达式应该通过验证");
            assertTest(LabelPolicyEvaluator.isValidExpression("level>3"),
                    "数值比较应该通过验证");
            assertTest(LabelPolicyEvaluator.isValidExpression("(env=\"prod\" OR env=\"test\") AND level<=5"),
                    "复杂表达式应该通过验证");

            // 无效表达式
            assertTest(!LabelPolicyEvaluator.isValidExpression("env="),
                    "无效表达式应该验证失败");
            assertTest(!LabelPolicyEvaluator.isValidExpression(""),
                    "空表达式应该验证失败");
            assertTest(!LabelPolicyEvaluator.isValidExpression(null),
                    "null表达式应该验证失败");
            assertTest(!LabelPolicyEvaluator.isValidExpression("env=\"prod\" AND"),
                    "不完整表达式应该验证失败");
            assertTest(!LabelPolicyEvaluator.isValidExpression("(env=\"prod\""),
                    "不匹配括号应该验证失败");

        } catch (Exception e) {
            System.out.println("✗ 语法验证测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testRealWorldScenarios() {
        try {
            // 场景1：生产环境访问控制
            SecurityLabel prodLabel = new SecurityLabel();
            prodLabel.setLabel("env", "prod");
            prodLabel.setLabel("region", "cn");
            prodLabel.setLabel("level", "4");

            String userPolicy1 = "env=\"prod\" AND level<=4";
            assertTest(LabelPolicyEvaluator.evaluate(userPolicy1, prodLabel),
                    "生产环境策略应该匹配");

            // 场景2：区域隔离
            String userPolicy2 = "region=\"cn\" OR region=\"asia\"";
            assertTest(LabelPolicyEvaluator.evaluate(userPolicy2, prodLabel),
                    "区域策略应该匹配");

            // 场景3：复杂权限控制
            String userPolicy3 = "(env=\"prod\" OR env=\"staging\") AND region!=\"us\" AND level<5";
            assertTest(LabelPolicyEvaluator.evaluate(userPolicy3, prodLabel),
                    "复杂权限策略应该匹配");

            // 场景4：多数据库访问
            SecurityLabel db1Label = new SecurityLabel();
            db1Label.setLabel("env", "prod");
            db1Label.setLabel("region", "cn");

            SecurityLabel db2Label = new SecurityLabel();
            db2Label.setLabel("env", "test");
            db2Label.setLabel("region", "us");

            String policy = "env=\"prod\" AND region=\"cn\"";
            assertTest(LabelPolicyEvaluator.evaluate(policy, db1Label),
                    "策略应该匹配db1");
            assertTest(!LabelPolicyEvaluator.evaluate(policy, db2Label),
                    "策略不应该匹配db2");

            // 场景5：开发测试环境
            SecurityLabel devLabel = new SecurityLabel();
            devLabel.setLabel("env", "dev");
            devLabel.setLabel("region", "cn");
            devLabel.setLabel("team", "backend");

            String devPolicy = "env!=\"prod\" AND team=\"backend\"";
            assertTest(LabelPolicyEvaluator.evaluate(devPolicy, devLabel),
                    "开发环境策略应该匹配");

        } catch (Exception e) {
            System.out.println("✗ 实际场景测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testPerformance() {
        try {
            SecurityLabel testLabel = createTestLabel();
            String complexPolicy = "(env=\"prod\" OR env=\"staging\") AND region=\"cn\" AND level>=2 AND role!=\"guest\"";

            // 性能测试
            int iterations = 1000;
            long startTime = System.nanoTime();

            for (int i = 0; i < iterations; i++) {
                LabelPolicyEvaluator.evaluate(complexPolicy, testLabel);
            }

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            double avgTime = (double) duration / iterations / 1_000_000; // 转换为毫秒

            System.out.println("  性能测试: " + iterations + "次评估，平均耗时: " + String.format("%.3f", avgTime) + "ms");
            assertTest(avgTime < 1.0, "平均评估时间应该小于1毫秒");

            // 复杂表达式性能测试
            String veryComplexPolicy = "((env=\"prod\" AND region=\"cn\") OR (env=\"test\" AND region=\"us\")) AND " +
                    "((level>2 AND level<6) OR (role=\"admin\" AND department=\"engineering\")) AND " +
                    "team!=\"guest\"";

            startTime = System.nanoTime();
            for (int i = 0; i < 100; i++) {
                LabelPolicyEvaluator.evaluate(veryComplexPolicy, testLabel);
            }
            endTime = System.nanoTime();
            double complexAvgTime = (double) (endTime - startTime) / 100 / 1_000_000;

            System.out.println("  复杂表达式性能: 100次评估，平均耗时: " + String.format("%.3f", complexAvgTime) + "ms");
            assertTest(complexAvgTime < 5.0, "复杂表达式评估时间应该小于5毫秒");

        } catch (Exception e) {
            System.out.println("✗ 性能测试异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static SecurityLabel createTestLabel() {
        SecurityLabel label = new SecurityLabel();
        label.setLabel("env", "prod");
        label.setLabel("region", "cn");
        label.setLabel("level", "3");
        label.setLabel("role", "admin");
        label.setLabel("department", "engineering");
        label.setLabel("team", "backend");
        return label;
    }

    private static void assertTest(boolean condition, String description) {
        totalTests++;
        if (condition) {
            passedTests++;
            System.out.println("✓ " + description);
        } else {
            System.out.println("✗ " + description);
        }
    }
}