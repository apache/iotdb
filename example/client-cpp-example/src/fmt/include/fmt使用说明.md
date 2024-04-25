
本文说明如何以`HEADER_ONLY`的方式使用[fmt](https://github.com/fmtlib/fmt)库。

 - [头文件](#包含头文件)
 - [格式化](#格式化)
   - [生成字符串](#生成字符串)
   - [对齐](#对齐)
   - [类型格式化](#类型格式化)
   - [打印容器](#打印容器)
   - [打印时间](#打印时间)
- [编译时错误检查](#编译时错误检查)


# 包含头文件
头文件的路径是`$/MACS_Platform_V7/01_dev/01_Platform/02_HMI/10_ThirdParty/fmt/include`

然后在引入任何fmt头文件之前定义`FMT_HEADER_ONLY`宏。
```cpp
#define FMT_HEADER_ONLY
#include <fmt/format.h>
```
对于CMAKE工程，可以通过`add_definitions(-D FMT_HEADER_ONLY)`来完成。


# 格式化

## 生成字符串
不需要关心数据类型，直接按位置输出即可
```cpp
std::string s = fmt::format("The answer is {}.", 42);

fmt::format("{0}, {1}, {2}", 'a', 'b', 'c');
// Result: "a, b, c"

fmt::format("{}, {}, {}", 'a', 'b', 'c');
// Result: "a, b, c"

// 次序可调
fmt::format("{2}, {1}, {0}", 'a', 'b', 'c');
// Result: "c, b, a"

fmt::format("{0}-{1}-{0}", "abra", "cad");
// Result: "abra-cad-abra"
```

## 对齐
```cpp
fmt::format("{:<30}", "left aligned");
// Result: "left aligned                  "

fmt::format("{:>30}", "right aligned");
// Result: "                 right aligned"

fmt::format("{:<{}}", "left aligned", 30);
// Result: "left aligned                  "

fmt::format("{:^30}", "centered");
// Result: "           centered           "

fmt::format("{:*^30}", "centered");  //把*作为填充字符
// Result: "***********centered***********"

fmt::format("{:.{}f}", 3.14, 1);
// Result: "3.1"
```

## 类型格式化
```cpp
fmt::format("int: {0:d};  hex: {0:x};  oct: {0:o}; bin: {0:b}", 42);
// Result: "int: 42;  hex: 2a;  oct: 52; bin: 101010"
// with 0x or 0 or 0b as prefix:

fmt::format("int: {0:d};  hex: {0:#x};  oct: {0:#o};  bin: {0:#b}", 42);
// Result: "int: 42;  hex: 0x2a;  oct: 052;  bin: 0b101010"
```


## 打印容器
```cpp
#include <vector>
#include <fmt/ranges.h>

int main() 
{
  std::vector<int> v = {1, 2, 3};
  fmt::print("{}\n", v);
}
```

## 打印时间
```cpp
#include <fmt/chrono.h>
std::time_t t = std::time(nullptr);

// "The date is 2020-11-10." (with the current date):
fmt::print("The date is {:%Y-%m-%d}.", fmt::localtime(t));
```

# 编译时错误检查
```cpp
string s1 = fmt::format("{:s}", "123");
s1 = fmt::format("{}", "123");
// 下面这条语句在编译时不报错，但运行时出错
// string s2 = fmt::format("{:s}",123);

std::string s3 = fmt::format(FMT_STRING("{:s}"), "foo");

// 下面这条语句在编译时错误
// std::string s4 = fmt::format(FMT_STRING("{:d}"), "foo");
```

# 打印其他
