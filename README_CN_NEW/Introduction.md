# 介绍

**dolphindb** 是 DolphinDB 的官方 Python API，用于连接 DolphinDB 服务端和 Python 客户端，从而实现数据的双向传输和脚本的调用执行。dolphindb 可以方便您在 Python 环境中调用 DolphinDB 进行数据的处理、分析和建模等操作，利用其优秀的计算性能和强大的存储能力来帮助您加速数据的处理和分析。

本手册共提供三大章节——快速开始，基本操作和进阶操作。

* 快速开始章节将介绍 dolphindb 的安装说明、简单示例和常用操作。
* 基本操作章节将介绍使用 dolphindb 的基本操作，如 session(会话)、DBConnectionPool（连接池）、追加数据、流订阅（基本）和异步写入的相关方法、注意事项和使用示例等。
* 进阶操作章节将详细说明类型转换、多种上传和写入数据方法、流订阅（进阶）、面向对象操作数据库的方法，以及其他功能。

dolphindb 提供了多种接口函数，可用于连接服务器、执行脚本、发送消息等。此外，dolphindb 支持数据的批量处理和异步执行，以及多种数据类型的交互，如 pandas.DataFrame、arrow.Table 等。dolphindb 支持 Linux(x86_64, arm)、Windows、MacOS(arm64, x86_64) 平台和 Python 3.6 - 3.10 版本，其使用 Pybind11 编写 C++ 库，从而优化后台多线程的处理，极大提高了数据交互的性能。

若您对本手册有任何宝贵意见，诚邀您通过 [DolphinDB 社区](https://ask.dolphindb.net) 与我们进行反馈交流。

# 目录

**第一章 快速开始**

* 1.1 [安装](./1_QuickStart/1.1_Install.md)
* 1.2 [快速上手](./1_QuickStart/1.2_Demo.md)

**第二章 基本操作**

* 2.1 session（会话）
  * 2.1.1 [创建](./2_BasicOperations/2.1_Session/2.1.1_Constructor.md)
  * 2.1.2 [连接](./2_BasicOperations/2.1_Session/2.1.2_Connect.md)
  * 2.1.3 [常用方法](./2_BasicOperations/2.1_Session/2.1.3_OtherParams.md)
* 2.2 DBConnectionPool（连接池）
  * 2.2.1 [创建](./2_BasicOperations/2.2_DBConnectionPool/2.2.1_Constructor.md)
  * 2.2.2 [方法介绍](./2_BasicOperations/2.2_DBConnectionPool/2.2.2_AsyncMethodsAndOthers.md)
* 2.3 追加数据
  * 2.3.1 [tableAppender](./2_BasicOperations/2.3_AutoFitTableAppender/2.3.1_TableAppender.md)
  * 2.3.2 [tableUpsert](./2_BasicOperations/2.3_AutoFitTableAppender/2.3.2_TableUpserter.md)
  * 2.3.3 [PartitionedTableAppender](./2_BasicOperations/2.3_AutoFitTableAppender/2.3.3_PartitionedTableAppender.md)
* 2.4 [流订阅（基本）](./2_BasicOperations/2.4_Subscription/2.4_Subscription.md)
* 2.5 同步写入
  * 2.5.1 [session 异步提交](./2_BasicOperations/2.5_AsyncWrites/2.5.1_SessionAsyncMode.md)
  * 2.5.2 [MultithreadedTableWriter](./2_BasicOperations/2.5_AsyncWrites/2.5.2_MultithreadedTableWriter.md)
  * 2.5.3 [BatchTableWriter](./2_BasicOperations/2.5_AsyncWrites/2.5.3_BatchTableWriter.md)

**第三章 进阶操作**

* 3.1 [类型转换](./3_AdvancedOperations/3.1_DataTypeCasting/3.1.0_TypeCasting.md)
  * 3.1.1 [PROTOCOL_DDB 协议](./3_AdvancedOperations/3.1_DataTypeCasting/3.1.1_PROTOCOL_DDB.md)
  * 3.1.2 [PROTOCOL_PICKLE 协议](./3_AdvancedOperations/3.1_DataTypeCasting/3.1.2_PROTOCOL_PICKLE.md)
  * 3.1.3 [PROTOCOL_ARROW 协议](./3_AdvancedOperations/3.1_DataTypeCasting/3.1.3_PROTOCOL_ARROW.md)
  * 3.1.4 [强制转换](./3_AdvancedOperations/3.1_DataTypeCasting/3.1.4_ForceTypeCasting.md)
* 3.2 [多种上传和写入方式的对比](./3_AdvancedOperations/3.2_WriteOptions/3.2_WriteOptions.md)
* 3.3 [流订阅（进阶）](./3_AdvancedOperations/3.3_SubscriptionOptions/3.3_SubscriptionOptions.md)
* 3.4 面向对象操作数据库
  * 3.4.1 [数据库](./3_AdvancedOperations/3.4_ObjectOrientedOperationsOnDdbOBjects/3.4.1_Database.md)
  * 3.4.2 [数据表](./3_AdvancedOperations/3.4_ObjectOrientedOperationsOnDdbOBjects/3.4.2_Table.md)
* 3.5 [其他](./3_AdvancedOperations/3.5_OtherFunctions/3.5_OtherFunctions.md)
