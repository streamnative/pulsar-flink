# Pulsar Flink Connector
Pulsar Flink connector实现了使用 [Apache Pulsar](https://pulsar.apache.org) 和 [Apache Flink](https://flink.apache.org)进行弹性数据处理的功能。

# 前提条件

- Java 8或更高
- Flink 1.9.0 或更高
- Pulsar 2.4.0 或更高

# 基本信息

## 客户端

现在支持有Flink多个版本：

- Flink 1.9 - 1.10 维护在分支`flink-1.9`

- Flink 1.11 维护在分支`flink-1.11`

- Flink 1.12 是当前的主力支持的版本，在`master`分支

  >  由于Flink的API变化较大，我们主要在master进行新特性的开发，其余分支以Bug修复为主。



对于使用SBT、Maven、Gradle的项目，可以使用下面参数设置到您的项目。

- `FLINK_VERSION`参数现在有`1.9`、`1.11`、`1.12`可选。

- `SCALA_BINARY_VERSION`参数很flink使用的scala版本有关，现有`2.11`、`2.12`可选。
- `PULSAR_FLINK_VERSION`是本连接器的版本。通常只有`2.7.0`三位的版本，但提供bug修复时会使用四位版本`2.7.0.1`。

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{FLINK_VERSION}}
    version = {{PULSAR_FLINK_VERSION}}
```
该Jar包位于 [Bintray Maven repository of StreamNative](https://dl.bintray.com/streamnative/maven)。


Maven项目可以加入仓库配置到您的`pom.xml`，内容如下：

```xml
  <repositories>
    <repository>
      <id>central</id>
      <layout>default</layout>
      <url>https://repo1.maven.org/maven2</url>
    </repository>
    <repository>
      <id>bintray-streamnative-maven</id>
      <name>bintray</name>
      <url>https://dl.bintray.com/streamnative/maven</url>
    </repository>
  </repositories>
```
Gradle项目可以在`build.gradle`中添加仓库的配置，内容如下：

```groovy
repositories {
        maven {
            url 'https://dl.bintray.com/streamnative/maven'
        }
}
```

对于maven项目，要构建包含库和pulsar flink连接器所需的所有依赖关系的应用程序JAR，您可以使用以下shade插件定义模板：

```xml
<plugin>
  <!-- Shade all the dependencies to avoid conflicts -->
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>${maven-shade-plugin.version}</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <createDependencyReducedPom>true</createDependencyReducedPom>
        <promoteTransitiveDependencies>true</promoteTransitiveDependencies>
        <minimizeJar>false</minimizeJar>

        <artifactSet>
          <includes>
            <include>io.streamnative.connectors:*</include>
            <!-- more libs to include here -->
          </includes>
        </artifactSet>
        <filters>
          <filter>
            <artifact>*:*</artifact>
            <excludes>
              <exclude>META-INF/*.SF</exclude>
              <exclude>META-INF/*.DSA</exclude>
              <exclude>META-INF/*.RSA</exclude>
            </excludes>
          </filter>
        </filters>
        <transformers>
          <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
          <transformer implementation="org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer" />
        </transformers>
      </configuration>
    </execution>
  </executions>
</plugin>
```

对于gradle项目，要构建包含库和pulsar flink连接器所需的所有依赖关系的应用程序JAR，您可以使用以下[shade](https://imperceptiblethoughts.com/shadow/)插件定义模板：

```groovy
buildscript {
    repositories {
        jcenter()
    }
    dependencies {
        classpath 'com.github.jengelman.gradle.plugins:shadow:6.0.0'
    }
}

apply plugin: 'com.github.johnrengelman.shadow'
apply plugin: 'java'
```





## 部署

### Client library
与任何Flink应用程序一样，`./bin/flink run`用于编译和启动您的应用程序。

如果您已经使用上面的shade插件构建了一个包含依赖的jar，则可以使用`--classpath`将您的jar添加到`flink run` 中。

> #### Note
> 路径的格式必须是协议（例如， `file://`），并且该路径在所有节点上都可以访问。

栗子🌰

```
$ ./bin/flink run
  -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
  ...
```

### Scala REPL
在交互式的Scala shell中进行尝试 `bin/start-scala-shell.sh`，你可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

栗子🌰

```
$ ./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
有关使用CLI提交应用程序的更多信息，请参考 [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/cli.html).


### SQL Client
要使用 [SQL Client](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html 并编写SQL查询操作Pulsar中的数据，你可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

栗子🌰
```
$ ./bin/sql-client.sh embedded --jar pulsar-flink-connector-{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
> 如果您把我们连接器的jar放到`$FLINK_HOME/lib`下，请不要再使用`--jar`指定连接器的包。

默认情况下，要在SQL Client中使用Pulsar目录并在启动时自动进行注册，SQL Client会从环境文件`./conf/sql-client-defaults.yaml`中读取其配置。 您需要在此YAML文件的`catalogs`部分中添加Pulsar目录：

```yaml
catalogs:
- name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
    format: json
```





# Stream环境

## Source

Flink的Pulsar消费者被称为`FlinkPulsarSource<T>`。 它提供对一个或多个Pulsar主题的访问。

它的构造方法有以下参数：

1. 连接Pulsar实例使用的服务地址`serviceUrl`和管理地址`adminUrl`。
2. 使用 `FlinkPulsarSource`时，需要设置`PulsarDeserializationSchema<T>`。
3. Properties参数，用于配置Pulsar Consumer的行为。
   Properties必须的参数，如下:

   - 这些参数中 `topic`, `topics` or `topicsPattern` 必须有一个存在值，且只能有一个。用于配置消费的Topic信息. (**`topics` 是用逗号`,`分隔的多个topic, `topicsPattern`是一个java正则表达式，可以匹配出若干topic**)
4. 设置消费模式可以通过 FlinkPulsarSource的setStartFromLatest、setStartFromEarliest、setStartFromSpecificOffsets、setStartFromSubscription等设置。使用setStartFromSubscription订阅者模式时，必须开启checkpoint功能。


例子:

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partition.discovery.interval-millis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, new PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING()), props);

// or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
source.setStartFromEarliest(); 

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```



## Sink

Pulsar生产者使用 `FlinkPulsarSink`实例。它允许将记录流写入一个或多个Pulsar Topic。

Example:

```java
PulsarSerializationSchema<Person> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Person.class))
.usePojoMode(Person. class, RecordSchemaType.JSON)
.setTopicExtractor(person -> null)
.build();
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
  serviceUrl,
  adminUrl,
  Optional.of(topic),      // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
  props,
  pulsarSerialization,
  PulsarSinkSemantic.AT_LEAST_ONCE);

stream.addSink(sink);
```



## PulsarDeserializationSchema

PulsarDeserializationSchema是连接器定义的Flink DeserializationSchema封装，可以灵活的操作Pulsar Message。

PulsarDeserializationSchemaWrapper是PulsarDeserializationSchema的简单实现，构造方法有两个参数: Flink DeserializationSchema 和解码的消息类型信息。

```
PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING())
```



> DataTypes类型来自flink的`table-common`模块。



## PulsarSerializationSchema

PulsarSerializationSchema 是 Flink SerializationSchema 的一个封装器，提供了更多的功能。大多数情况下，你不需要自己实现 PulsarSerializationSchema，我们提供 PulsarSerializationSchemaWrapper 来包装一个 Flink SerializationSchema 成为 PulsarSerializationSchema。

PulsarSerializationSchema 使用构建器模式，你可以调用 setKeyExtractor 或 setTopicExtractor 来满足从每个消息中提取密钥和自定义目标Topic的需求。

特别是，由于 Pulsar 在内部维护着自己的 Schema 信息，所以我们的消息在写入 Pulsar 时必须能够导出一个 SchemaInfo。 useSpecialMode、useAtomicMode、usePojoMode 和 useRowMode 方法可以帮助您快 速构建 Pulsar 所需的 Schema 信息。你必须在这四种模式中只选择一种。

- SpecialMode：直接指定 Pulsar 中的 `Schema<?>`模式，请保证这个Schema和你设置 Flink SerializationSchema是兼容的。

- AtomicMode：对于一些原子类型的数据，传递 AtomicDataType 的类型，如`DataTypes.INT()`，它将对应 Pulsar 中的 `Schema<Integer>`。
- PojoMode：你需要传递一个自定义 Class 对象和 Json 或 Arvo 中的一个来指定构建复合类型 Schema 的方式。例如 `usePojoMode(Person.class, RecordSchemaType.JSON)`。

- RowMode：一般来说，你不会使用这个模式，它用于我们内部的 Table&SQL API 的实现。



## 容错

启用Flink的检查点后，`FlinkPulsarSink`可以提供at-least-once、exactly-once的交货保证。

除了启用Flink的检查点之外，您还应该配置`setLogFailuresOnly(boolean)` 和 `setFlushOnCheckpoint(boolean)`。

*`setFlushOnCheckpoint(boolean)`*：默认情况下，它设置为`true`。启用此功能后，写入pulsar记录会在本次checkpoint snapshotState时执行。这样可以确保checkpoint之前的所有记录写给pulsar了。注意必须同时开启flink的at-least-once设置。



# Table环境

Pulsar-flink连接器全面支持了Table功能，覆盖了以下列表：

- SQL、DDL
- Catalog

## SQL、DDL

SQL实例

```sql
CREATE TABLE pulsar (
  `physical_1` STRING,
  `physical_2` INT,
  `eventTime` TIMESTAMP(3) METADATA,
  `properties` MAP<STRING, STRING> METADATA ,
  `topic` STRING METADATA VIRTUAL,
  `sequenceId` BIGINT METADATA VIRTUAL,
  `key` STRING ,
  `physical_3` BOOLEAN
) WITH (
  'connector' = 'pulsar',
  'topic' = 'persistent://public/default/topic82547611',
  'key.format' = 'raw',
  'key.fields' = 'key',
  'value.format' = 'avro',
  'service-url' = 'pulsar://localhost:6650',
  'admin-url' = 'http://localhost:8080',
  'scan.startup.mode' = 'earliest' 
)

INSERT INTO pulsar 
VALUES
 ('data 1', 1, TIMESTAMP '2020-03-08 13:12:11.123', MAP['k11', 'v11', 'k12', 'v12'], 'key1', TRUE),
 ('data 2', 2, TIMESTAMP '2020-03-09 13:12:11.123', MAP['k21', 'v21', 'k22', 'v22'], 'key2', FALSE),
 ('data 3', 3, TIMESTAMP '2020-03-10 13:12:11.123', MAP['k31', 'v31', 'k32', 'v32'], 'key3', TRUE)
 
SELECT * FROM pulsar
```

SQL中，完整支持了物理字段、计算列、METADATA等特性。



### DDL配置支持列表

| 参数                          | 默认值        | 描述                                                         | 必填 |
| ----------------------------- | ------------- | ------------------------------------------------------------ | ---- |
| connector                     | null          | 使用的连接器，可选择pulsar和upsert-pulsar。                  | 是   |
| topic                         | null          | 输入或输出的topic，多个时使用半角逗号 `,` 连接。与`topic-pattern`二选一 | 否   |
| topic-pattern                 | null          | 使用正则获得匹配的Topic。与`topic`二选一。                   | 否   |
| service-url                   | null          | Pulsar broker服务地址                                        | 是   |
| admin-url                     | null          | Pulsar admin服务地址                                         | 是   |
| scan.startup.mode             | latest        | Source的启动模式，可选项 earliest、latest、external-subscription、specific-offsets | 否   |
| scan.startup.specific-offsets | null          | 当使用specific-offsets时，必须指定消息偏移量                 | 否   |
| scan.startup.sub-name         | null          | 当使用订阅模式（external-subscription）时，必须设置。        | 否   |
| discovery topic interval      | null          | 分区发现的时间间隔，毫秒                                     | 否   |
| sink.message-router           | key-hash      | 写消息到Pulsar分区的路由方式，可选项key-hash、round-robin、自定义MessageRouter实现类的引用路径 | 否   |
| sink.semantic                 | at-least-once | Sink写出消息的保障级别。可选项at-least-once、exactly-once、none | 否   |
| properties                    | empty         | Pulsar可选的配置集，格式 `properties.key='value'`,具体参考 #配置参数 | 否   |
| key.format                    | null          | Pulsar Message的Key序列化格式，可选raw、avro、json等等       | 否   |
| key.fields                    | null          | 序列化Key时需要使用的SQL定义字段，多个按半角逗号 `,`连接。   | 否   |
| key.fields-prefix             | null          | 为键格式的所有字段定义一个自定义前缀，以避免名称与值格式的字段冲突。默认情况下，前缀为空。如果定义了自定义前缀，则Table模式和`'key.fields'`都将使用带前缀的名称。构造密钥格式的数据类型时，前缀将被删除，并且非前缀名称将在密钥格式内使用。 | 否   |
| format或value.format          | null          | Pulsar消息正文的序列化格式，支持json、avro等，更多参考Flink format。 | 是   |
| value.fields-include          | ALL           | Pulsar消息正文包含字段策略，可选项ALL, EXCEPT_KEY            | 否   |





### Pulsar Message元数据操作

METADATA标志用于读写Pulsar Message中的元数据。支持列表如下：

**R/W列定义了元数据字段是否可读（R）和/或可写（W）。只读列必须声明为VIRTUAL，以便在INSERT INTO操作中排除它们。**


| Key  | Data Type | Description | R/W  |
| ---- | --------- | ----------- | ---- |
| topic | STRING NOT NULL | Topic name of the Pulsar Message. | R    |
| messageId | BYTES NOT NULL | MessageId of the Pulsar Message. |R |
| sequenceId | BIGINT NOT NULL| Pulsar Message sequence Id | R |
| publishTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Pulsar message published time| R |
| eventTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Message Generation Time |R/W |
| properties | MAP<STRING, STRING> NOT NULL | Pulsar Message Extensions Information. | R/W |





## Catalog

Flink始终在当前目录和数据库中搜索表，视图和UDF。 要使用Pulsar Catalog并将Pulsar中的topic作为Flink中的表对待，您应该使用已在`./conf/sql-client-defaults.yaml`中定义的`pulsarcatalog`。 .

```java
tableEnv.useCatalog("pulsarcatalog")
tableEnv.useDatabase("public/default")
tableEnv.scan("topic0")
```

```SQL
Flink SQL> USE CATALOG pulsarcatalog;
Flink SQL> USE `public/default`;
Flink SQL> select * from topic0;
```

以下配置在环境文件中是可选的，或者可以使用`SET`命令在SQL客户端会话中覆盖。

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>`default-database`</td>
  <td>默认数据库名称。</td>
  <td>public/default</td>
  <td>使用Pulsar catalog时，Pulsar中的topic被视为Flink中的表，因此，`database`是`tenant/namespace`的另一个名称。 数据库是表查找或创建的基本路径。</td>
  </tr>
  <tr>
  <td>`table-default-partitions`</td>
  <td>Topic默认分区</td>
  <td>5</td>
  <td>使用Pulsar catalog时，Pulsar中的topic被视为Flink中的表。创建Topic时，设置的分区大小。</td>
  </tr>
</table>


更多参数明细参考DDL中的配置项

注意：由于删除操作具有危险性，Catalog中删除`tenant/namespace`、`topic`操作暂不被支持。




# 高级特性



## Pulsar原生类型

Pulsar自身提供了一些基本的原生类型，如果你需要使用原生类型，可以以下列方式支持



### Stream API环境

PulsarPrimitiveSchema是`PulsarDeserializationSchema`、`PulsarSerializationSchema`接口的实现。

您可以以相似的方法创建需要的实例`new PulsarSerializationSchema(String.class)`。



### Table 环境

我们创建了一款名为`atomic`新的Flink format组件，您可以在SQL的format使用它。在Source中，它会将Pulsar原生类型翻译成只有一列值得RowData。在Sink中，它会将RowData的第一列翻译成Pulsar原生类型写入Pulsar。



## Upsert Pulsar

Flink 社区用户对 Upsert 模式消息队列有很高的需求，主要原因有三个：

- 将 Pulsar Topic 解释为一个 changelog 流，它将带有键的记录解释为 upsert 事件；
-  作为实时管道的一部分，将多个流连接起来进行充实，并将结果存储到 Pulsar Topic 中，以便以后进行进一步的计算。但结果可能包含更新事件。
- 作为实时管道的一部分，聚合数据流并将结果存储到 Pulsar Topic 中，以便以后进行进一步计算。但是结果可能包含更新事件。
  基于这些需求，我们也实现了对 Upsert Pulsar 的支持。使用这个功能，用户能够以 upsert 的方式从 Pulsar 主题中读取数据和向 Pulsar 主题写入数据。



在SQL DDL定义中，您将connector设置为upsert-pulsar，即可使用Upsert Pulsar连接器。

在配置方面，必须指定Table的主键，且`key.fields`不能使用。

作为 source，upsert-pulsar 连接器生产 changelog 流，其中每条数据记录代表一个更新或删除事件。更准确地说，数据记录中的 value 被解释为同一 key 的最后一个 value 的 UPDATE，如果有这个 key（如果不存在相应的 key，则该更新被视为 INSERT）。用表来类比，changelog 流中的数据记录被解释为 UPSERT，也称为 INSERT/UPDATE，因为任何具有相同 key 的已存在行都会被覆盖。另外，value 为空的消息将会被视作为 DELETE 消息。

作为 sink，upsert-pulsar 连接器可以消费 changelog 流。它会将 INSERT/UPDATE_AFTER 数据作为正常的 Pulsar 消息写入，并将 DELETE 数据以 value 为空的 Pulsar 消息写入（表示对应 key 的消息被删除）。Flink 将根据主键列的值对数据进行分区，从而保证主键上的消息有序，因此同一主键上的更新/删除消息将落在同一分区中。



## Pulsar Key-Shared

高性能的有序消息队列在有些场景下，用户需要消息严格保证消息顺序，才能保证业务处理正确。通常在消息严格保序的情况下，只能同时有一个消费者消费消息，才能保证顺序。这样会导致消息的吞吐量大幅度降低。Pulsar 为这样的场景设计了 Key-Shared 订阅模式，通过对消息增加 Key，将相同 Key Hash 的消息路由到同一个消息者上，这样既保证了消息的消息顺序，又提高了吞吐量。

我们在 Pulsar Flink 连接器中也添加了对该功能的支持。可以通过配置参数`enable-key-hash-range=true` 启用这个功能。开启功能后，会根据任务的并行度划分每个消费者处理的 Key Hash 范围。

## 配置参数

该参数对应StreamAPI中的FlinkPulsarSource、FlinkPulsarSink构造参数中的Properties对象，Table模式下的配置properties参数。

| 参数                                 | 默认值        | 描述                                                         | 生效范围     |
| ------------------------------------ | ------------- | ------------------------------------------------------------ | ------------ |
| topic                                | null          | pulsar topic                                                 | source       |
| topics                               | null          | 半角逗号连接的多个pulsar topic                               | source       |
| topicspattern                        | null          | java正则匹配多的多个pulsar topic                             | source       |
| partition.discovery.interval-millis  | -1            | 自动发现增减topic，毫秒。-1表示不开启。                      | source       |
| clientcachesize                      | 100           | 缓存pulsar client数量                                        | source、sink |
| auth-plugin-classname                | null          | Pulsar client鉴权类                                          | source、sink |
| auth-params                          | null          | Pulsar client鉴权参数                                        | source、sink |
| flushoncheckpoint                    | true          | 在flink snapshotState时写出消息到pulsar                      | sink         |
| failonwrite                          | false         | sink出错时，继续确认消息                                     | sink         |
| polltimeoutms                        | 120000        | 等待获取下一条消息的超时时间，毫秒                           | source       |
| failondataloss                       | true          | 数据丢失时是否失败                                           | source       |
| commitmaxretries                     | 3             | 向pulsar消息偏移offset时，最大重试次数                       | source       |
| scan.startup.mode                    | latest        | earliest、latest，订阅者消费消息的位置                       | source       |
| enable-key-hash-range                | false         | 开启Pulsar Key-Shared模式                                    | source       |
| pulsar.reader.*                      |               | pulsar consumer的详细配置，项目可参考[Pulsar Reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader) | source       |
| pulsar.reader.subscriptionRolePrefix | flink-pulsar- | 未指定订阅者时，自动创建订阅者名称的前缀                     | source       |
| pulsar.reader.receiverQueueSize      | 1000          | 接收队列大小                                                 | source       |
| pulsar.producer.*                    |               | pulsar consumer的详细配置，项目可参考[Pulsar Producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer) | Sink         |
| pulsar.producer.sendTimeoutMs        | 30000         | 发送消息时的超时时间，毫秒                                   | Sink         |
| pulsar.producer.blockIfQueueFull     | false         | 生产者写入消息，队列满时，阻塞方法，而不是抛出异常           | Sink         |

`pulsar.reader.*`和`pulsar.producer.*`指定更详细的配置pulsar的行为，*替换为pulsar中的配置名，内容参考表中的链接。



在DDL语句中，使用的样例如下:

```
'properties.pulsar.reader.subscriptionRolePrefix' = 'pulsar-flink-',
'properties.pulsar.producer.sendTimeoutMs' = '30000',
```



## 身份验证配置

对于配置了身份验证的Pulsar实例，可以使用常规Pulsar客户端类似的方式设置Pulsar Flink连接器。

对于FlinkPulsarSource、FlinkPulsarSink，您有两种方式设置认证

- 构造参数 `Properties`

  ```java
  props.setProperty(PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY, "org.apache.pulsar.client.impl.auth.AuthenticationToken");
  props.setProperty(PulsarOptions.AUTH_PARAMS_KEY, "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.2AgtxHe8-2QBV529B5DrRtpuqP6RJjrk21Mhnomfivo");
  ```

- 构造参数 `ClientConfigurationData`，它的优先级高于`Properties`。

  ```java
  ClientConfigurationData conf = new ClientConfigurationData();
  conf.setServiceUrl(serviceUrl);
  conf.setAuthPluginClassName(className);
  conf.setAuthParams(params);
  ```

身份验证详细配置请参考 [Pulsar Security](https://pulsar.apache.org/docs/en/security-overview/) 。





# 构建 Pulsar Flink Connector
如果要构建Pulsar Flink连接器，期望从Pulsar读取数据或者将结果写入Pulsar，请按照以下步骤操作。
1. 检出源代码
    ```bash
    $ git clone https://github.com/streamnative/pulsar-flink.git
    $ cd pulsar-flink
    ```

2. 安装Docker

   Pulsar-flink连接器正在使用[Testcontainers](https://www.testcontainers.org/)进行集成测试。 为了运行集成测试，请确保已安装 [Docker](https://docs.docker.com/docker-for-mac/install/)。

3. 设置Java版本

   在`pom.xml`修改 `java.version` 和 `java.binary.version`。
   > #### Note
   > Java版本应与您使用的flink的Java版本一致。

4. 构建项目
    ```bash
    $ mvn clean install -DskipTests
    ```

5. 运行测试
    ```bash
    $ mvn clean install
    ```
安装完成后，在本地maven repo和`target`目录下都会生成一个包含依赖的jar。

> 注意：如果你使用 intellij IDEA 来调试这个项目，可能会遇到找不到package `org.apache.pulsar.shade.org.bookkeeper.ledger` 错误。 解决办法:先运行 ` mvn clean install -DskipTests` 安装 jar 到本地仓库，然后在项目上忽略maven模块`managed-ledger-shaded`。刷新项目后错误消失。
