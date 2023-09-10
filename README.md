# 工程简介
本工程是【复赛】比赛时选手需要下载的工程，选手实现工程内 TSDBEngineImpl 类。
评测程序会将选手提交的修改后的本工程代码进行编译并调用选手实现的 TSDBEngineImpl 类的接口进行测试。
其他参赛说明及本文档均未说明或不清楚的问题可询比赛客服。
  

# 注意事项
1. 日志必须使用 std::out 或 std::err，打印到其他地方正式测评时可能无法透传出来。
2. 建议不要频繁打印日志，正式环境中日志的实现可能会产生同步、异步 IO 行为，影响选手成绩，正式比赛中对每次提交运行的总日志大小有限制。
3. 选手只能修改 TSDBEngineImpl 类，选手可以增加其他类，但不能修改已定义的其他文件。
4. 大赛官方不推荐通过链接使用三方库，如果需要使用三方库建议拷贝源码到工程中，如果需要链接使用请自行调试我们不提供支持。
5. 自建的头文件请放入 include 文件夹，如果有子文件夹，引入时，请携带 include 为根的相对路径，因为正式编译时头文件路径只认 include 目录：
   + 例如，头文件为 include/header_1.h，引入方式为 #include "header_1.h"
   + 例如，头文件为 include/my_folder/header_2.h，引入方式为 #include "my_folder/header_2.h"
   + 除了以 include 为根的相对路径，以本（cpp、c、cc）文件所在目录为相对路径引入头文件也是可以的
6. 自建的 cpp 文件只能放入 source 目录下，可以存在子目录，因为正式编译时 cpp 搜寻目录只认 source 目录。
7. 选手提交时，将本工程打包到一个 zip 包中，zip 包应将整个 lindorm-tsdb-contest-cpp 目录打包，而不是将该目录下的内容打包，即最终 zip 包根目录中只有 lindorm-tsdb-contest-cpp 一个目录：
    + cd .xxxxx/lindorm-tsdb-contest-cpp
    + cd .. # 退回上级目录
    + add directory to zip package root: ./lindorm-tsdb-contest-cpp
8. 实际评测程序不会依赖本工程下的 CMakeLists.txt，因此选手可以随意修改 cmake 的属性以满足本地调试需要。
9. 基础代码选手不可修改的部分（如一些结构体等）我们已经事先进行了 UT 测试，但仍不排除存在 BUG 的可能性，如果选手发现了问题影响参赛，请及时与我们联系。
10. 接口运行时抛出异常、返回值为异常返回值的，评测会立即结束，并视为运行失败。
  

# 工程结构说明
+ include：放置头文件的目录。
+ source: 放置 cpp 文件的目录。
  

# 评测程序流程参考
1. 选手拉取本仓库，并实现 TSDBEngineImpl.cpp 中的各个接口函数。
2. 选手打包，并上传。
3. 评测程序将从 source 目录中扫描加载所有 cpp 文件。除 Linux 内置环境的 include 路径外，设置 include 根目录为头文件 -I 所在唯一位置（请选手注意对 include 子文件夹中头文件的引用路径）。
4. 评测程序可能会执行的操作：
    1. 写入测试。
    2. 正确性测试。
    3. 重启，清空缓存。
    4. 重新通过先前的数据目录重启数据库，数据库需要加载之前持久化的数据。
    5. 正确性测试。
    6. 读取性能测试。
    7. 压缩率测试。
  