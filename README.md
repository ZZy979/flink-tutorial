# Flink入门教程示例代码

博客链接：<https://zzy979.github.io/posts/flink-tutorial/>

示例代码
## 批处理
* [单词计数](src/main/java/com/example/batch/WordCount.java)

## 流处理
* [单词计数](src/main/java/com/example/streaming/SocketWindowWordCount.java)
* [简单示例](src/main/java/com/example/streaming/AdultFilter.java)
* [无状态转换示例](src/main/java/com/example/streaming/StatelessTransformationExample.java)
* [分组聚合示例](src/main/java/com/example/streaming/KeyedStreamExample.java)
* [状态示例：事件去重](src/main/java/com/example/streaming/EventDeduplicator.java)
* [连接流示例：单词过滤](src/main/java/com/example/streaming/WordFilter.java)
* [窗口函数示例：处理传感器读数](src/main/java/com/example/streaming/SensorReadingProcessor.java)
* [join流示例](src/main/java/com/example/streaming/JoiningStreams.java)

## Table API
* [单词计数](src/main/java/com/example/table/WordCount.java)
* [Table查询示例：用户订单数据](src/main/java/com/example/table/UserOrderData.java)
* [DataStream-Table转换示例](src/main/java/com/example/table/DataStreamTableConversion.java)
* [ChangelogStream-Table转换](src/main/java/com/example/table/ChangelogStreamTableConversion.java)
* [持续查询示例：用户点击事件](src/main/java/com/example/table/ContinuousQueryExample.java)
