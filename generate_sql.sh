#!/bin/bash

# 输入文件名
input_file="agg"
# 输出文件名
output_file="insert_statements.sql"

# 初始化 $value1
value1=1

# 清空或创建输出文件
> "$output_file"

# 逐行读取输入文件
while IFS= read -r line; do
    # 构造插入语句
    echo "INSERT INTO test VALUES($value1, 'd1', 'yellow', $line);" >> "$output_file"
    # 增加 $value1
    ((value1++))
done < "$input_file"

echo "SQL 插入语句已生成到 $output_file"
