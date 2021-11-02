# Pulsar Flink 连接器

Pulsar Flink 连接器使用 [Apache Pulsar](https://pulsar.apache.org) 和 [Apache Flink](https://flink.apache.org) 处理弹性数据。

# 前提条件

- Java 8 或更高版本
- Flink 1.9.0 或更高版本
- Pulsar 2.5.0 或更高版本

# 基本信息

本章节介绍 Pulsar Flink 连接器的基本信息。

## 客户端

基于新的版本定义模型我们对当前连接器支持的 Pulsar 和 Flink 的支持做了如下的细分：

| Flink 版本 | Pulsar client 版本 (最低版本) | 连接器分支                                                                 |
|:--------------|:---------------------------------|:---------------------------------------------------------------------------------|
| 1.9.x         | 2.5.x                            | [`release-1.9`](https://github.com/streamnative/pulsar-flink/tree/release-1.9)   |
| 1.10.x        | 2.5.x                            | [`release-1.10`](https://github.com/streamnative/pulsar-flink/tree/release-1.10) |
| 1.11.x        | 2.6.x                            | [`release-1.11`](https://github.com/streamnative/pulsar-flink/tree/release-1.11) |
| 1.12.x        | 2.7.x                            | [`release-1.12`](https://github.com/streamnative/pulsar-flink/tree/release-1.12) |


> **说明**  
> Flink API 每个版本变化较大。我们只在新版本 Flink 上开发新特性。旧 Flink 版本只做 Bug 修复。

## 版本定义

我们现在将 jar 包发布到 Maven 中央库，你可以直接在 Maven、Gradle 和 SBT 里引用。包含两类连接器，`pulsar-flink-connector_2.11` 是针对 Scala 2.11 运行环境，而 `pulsar-flink-connector_2.12` 是给 Scala 2.12 运行环境使用的。这种命名的方式和 Flink 仓库的连接器保持一致。对于连接器的版本号，我们使用 4 位数字来定义，前三位代表能使用的 Flink 版本号，最后一位是我们自增的迭代版本。

这样的版本命名便于用户选取合适的连接器用于项目中，也便于我们去发版。新版连接器中不再直接包含 `pulsar-client-all`，我们通过依赖的方式将其引入你的项目里，所以你可以修改依赖的 `pulsar-client-all` 版本来达成多版本兼容。

## Maven 项目

对于 Maven 项目，用户可以在 `pom.xml` 中添加连接器依赖，内容如下。其中 `scala.binary.version` 和 Flink 的依赖定义一致，你可以直接在 properties 属性中定义。`${pulsar-flink-connector.version}`可以基于你需要的 Flink 版本去选取，也定义在 properties 属性里。

```xml
<dependency>
    <groupId>io.streamnative.connectors</groupId>
    <artifactId>pulsar-flink-connector_${scala.binary.version}</artifactId>
    <version>${pulsar-flink-connector.version}</version>
</dependency>
```

对于 Maven 项目，想要构建包含库和 Pulsar Flink 连接器所需的所有依赖关系的 JAR 包，用户可以使用以下 [shade](https://imperceptiblethoughts.com/shadow/) 插件定义模板：

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
            <include>org.apache.pulsar:*</include>
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

## Gradle项目

对于 Gradle 项目，用户需要确保 `build.gradle` 中添加了 Maven 中央仓库的配置，内容如下：

```groovy
repositories {
    mavenCentral()
}
```

对于 Gradle 项目，想要构建包含库和 Pulsar Flink 连接器所需的所有依赖关系的 JAR 包，用户可以使用以下[shade](https://imperceptiblethoughts.com/shadow/) 插件定义模板：

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

# 构建 Pulsar Flink 连接器

Pulsar Flink 连接器用来从 Pulsar 中读取数据或者将结果写入 Pulsar。如需构建 Pulsar Flink 连接器，请遵循以下步骤。

1. 克隆源代码。

  ```bash
  git clone https://github.com/streamnative/pulsar-flink.git
  cd pulsar-flink
  ```

2. 安装 Docker。

  Pulsar Flink 连接器使用[Testcontainers](https://www.testcontainers.org/) 进行集成测试。为了运行集成测试，请确保已安装 [Docker](https://docs.docker.com/docker-for-mac/install/)。

3. 设置 Java 版本。

  在 `pom.xml` 文件中修改 `java.version` 和 `java.binary.version` 参数。
  > **说明**  
  > Java 版本应与使用的 Flink 的 Java 版本保持一致。

4. 构建项目。

  ```bash
  mvn clean install -DskipTests
  ```

5. 运行测试。

  ```bash
  mvn clean install
  ```

安装完成后，在本地 Maven 项目库和 `target` 目录下都会生成一个包含依赖的 JAR 包。

# 部署 Pulsar Flink 连接器

本章节介绍如何部署 Pulsar Flink 连接器。

## Client library

与其他 Flink 应用程序一样，`./bin/flink run` 命令用于编译和启动用户的应用程序。

如果用户已使用 shade 插件构建了一个包含依赖关系的 JAR 包，则可以使用 `--classpath` 参数将该 JAR 包添加到 `flink run` 中。

> **说明**  
> 路径必须采用协议格式（例如，`file://`），并且所有节点都可以访问该路径。

**举例**

```
./bin/flink run -c com.example.entry.point.ClassName file://path/to/jars/your_fat_jar.jar
```

## Scala REPL

在交互式的 Scala shell 中进行尝试 `bin/start-scala-shell.sh`，你可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

**举例**

```
./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```

有关使用 CLI 提交应用程序的更多信息，参见 [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/cli.html).

## SQL 客户端

如需使用 [SQL 客户端](https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/sqlClient.html)并编写 SQL 查询、操作 Pulsar 中的数据，用户可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

**举例**

```
./bin/sql-client.sh embedded --jar pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```

> **说明**
> 如果连接器的 JAR 包已经位于 `$FLINK_HOME/lib` 下，请不要再使用 `--jar` 参数指定连接器的 JAR 包。

默认情况下，要在 SQL 客户端中使用 Pulsar 目录并在启动时自动进行注册，SQL 客户端会从 `./conf/sql-client-defaults.yaml` 环境文件中读取其配置。用户需要在此 YAML 文件的 `catalogs` 部分中添加 Pulsar 目录，如下所示。

```yaml
catalogs:
- name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
    format: json
```

# 使用场景

本章节介绍 Pulsar Flink 连接器的使用场景。

## Stream 环境

本章节介绍如何在 Stream 环境中使用 Pulsar Flink 连接器。

### Source

在 Flink 中，Pulsar consumer 被称为 `FlinkPulsarSource<T>`。`FlinkPulsarSource<T>` 支持访问一个或多个 Pulsar 主题。

用户可以使用以下参数来构造 `FlinkPulsarSource<T>`。

- `serviceUrl` 和 `adminUrl`：用于连接 Pulsar 实例的服务地址和管理地址。
- `PulsarDeserializationSchema`：使用 `FlinkPulsarSource` 时，需要设置 `PulsarDeserializationSchema<T>`。
- `Properties`：用于配置 Pulsar Consumer 的行为。这些参数中，`topic`、 `topics` 或 `topicsPattern` 参数用于配置消费的 Topic 信息，必须配置取值，且同时只能配置其中一个参数。(**`topics` 参数支持使用逗号（,）分隔多个 Topic。`topicsPattern` 参数是一个 JAVA 正则表达式，可以匹配若干 Topic。**)
- `FlinkPulsarSource的setStartFromLatest`、`setStartFromEarliest`、`setStartFromSpecificOffsets`、`setStartFromSubscription`：用于配置订阅模式。设置为 `setStartFromSubscription` 订阅模式时，必须开启 checkpoint 功能。

**举例**

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partition.discovery.interval-millis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, PulsarDeserializationSchema.valueOnly(new SimpleStringSchema()), props);

// or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
source.setStartFromEarliest(); 

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```

### Sink

Pulsar producer 使用 `FlinkPulsarSink` 实例。`FlinkPulsarSink` 实例允许将记录流写入一个或多个 Pulsar Topic。

**举例**

```java
PulsarSerializationSchema<Person> pulsarSerialization = new PulsarSerializationSchemaWrapper.Builder<>(JsonSer.of(Person.class))
    .usePojoMode(Person. class, RecordSchemaType.JSON)
    .setTopicExtractor(person -> null)
    .build();
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
    serviceUrl,
    adminUrl,
    Optional.of(topic), // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
    props,
    pulsarSerialization,
    PulsarSinkSemantic.AT_LEAST_ONCE
);

stream.addSink(sink);
```

## PulsarDeserializationSchema

PulsarDeserializationSchema 是 Pulsar Flink 连接器定义的 Flink DeserializationSchema 封装，可以灵活地操作 Pulsar 消息。

PulsarDeserializationSchemaWrapper 是 PulsarDeserializationSchema 的简单实现。用户可以使用 Flink DeserializationSchema 和解码的消息类型信息构造 PulsarDeserializationSchema，如下所示。

```
PulsarDeserializationSchemaWrapper(new SimpleStringSchema(),DataTypes.STRING())
```

> **说明**  
> `DataTypes` 类型来自 Flink的 `table-common` 模块。

### PulsarSerializationSchema

PulsarSerializationSchema 是 Flink SerializationSchema 的一个封装器，实现更多功能。大多数情况下，用户无需自己实现 PulsarSerializationSchema。我们支持使用 PulsarSerializationSchemaWrapper 来将 Flink SerializationSchema 包装成为 PulsarSerializationSchema。

PulsarSerializationSchema 使用构建器模式。用户可以调用 `setKeyExtractor` 或 `setTopicExtractor`，从每个消息中提取密钥和自定义目标 Topic。

Pulsar 在内部维护着自己的 Schema 信息，所以我们的消息在写入 Pulsar 时必须能够导出一个 SchemaInfo。`useSpecialMode`、`useAtomicMode`、`usePojoMode` 和 `useRowMode` 方法可以帮助用户快速构建 Pulsar 所需的 Schema 信息。

- SpecialMode：直接指定 Pulsar 的 `Schema<?>` 模式。确保该 Schema 和 Flink SerializationSchema 相互兼容。
- AtomicMode：对于一些原子类型的数据，传递 AtomicDataType 的类型，如`DataTypes.INT()`。它对应 Pulsar 中的 `Schema<Integer>`。
- PojoMode：传递一个自定义 Class 对象和 JSON 或 Arvo 中用于指定构建复合类型 Schema 的方式。例如 `usePojoMode(Person.class, RecordSchemaType.JSON)`。
- RowMode：一般来说，用户不会使用这个模式。它用于我们内部的 Table&SQL API 的实现。

### 容错

启用 Flink 的 checkpoint 功能后，`FlinkPulsarSink` 可以实现 `at-least-once`、`exactly-once` 的交货保证。

除了启用 Flink 的 checkpoint 功能之外，用户还应该配置 `setLogFailuresOnly(boolean)` 和 `setFlushOnCheckpoint(boolean)` 参数。

> **说明**  
> *`setFlushOnCheckpoint(boolean)`*：默认情况下，设置为 `true`。启用此功能后，会在当前 checkpoint snapshotState 时记录写入 Pulsar 的数据。这样可以确保checkpoint 之前的所有数据都已经写入 Pulsar。但是，必须同时开启 Flink 的 `at-least-once` 设置。

## Table 环境

Pulsar Flink 连接器全面支持 Table 功能。

- SQL and DDL
- Catalog

### SQL 和 DDL

本章节介绍 SQL 配置和 DDL 配置。

### SQL 配置

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

SQL 全面支持物理字段、计算列、watermark、METADATA 等特性。

### DDL 配置

| 参数                          | 默认值        | 描述                                                         | 必填 |
| ----------------------------- | ------------- | ------------------------------------------------------------ | ---- |
| connector                     | null          | 使用的连接器，可选择pulsar和upsert-pulsar。               | 是   |
| topic                         | null          | 输入或输出的 Topic。如果有多个 Topic，使用半角逗号 （, 连接。与 `topic-pattern` 参数互斥。 | 否   |
| topic-pattern                 | null          | 使用正则获得匹配的 Topic。与`topic` 参数互斥。                 | 否   |
| service-url                   | null          | Pulsar broker 的服务地址。                                      | 是   |
| admin-url                     | null          | Pulsar Admin 的服务地址                                         | 是   |
| scan.startup.mode             | latest        | Source 的启动模式。支持 `earliest`、`latest`、`external-subscription`、`specific-offsets` 选项。 | 否   |
| scan.startup.specific-offsets | null          | 当使用 `specific-offsets ` 参数时，必须指定消息偏移量。               | 否   |
| scan.startup.sub-name         | null          | 当使用 `external-subscription` 参数时，必须设置该参数。        | 否   |
| discovery topic interval      | null          | 分区发现的时间间隔，单位为毫秒。                                     | 否   |
| sink.message-router           | key-hash      | 写消息到 Pulsar 分区的路由方式。支持 `key-hash`、 `round-robin`、自定义 MessageRouter 实现类的引用路径。 | 否   |
| sink.semantic                 | at-least-once | Sink 写出消息的保障级别。支持 `at-least-once`、`exactly-once`、`none` 选项。 | 否   |
| properties                    | empty         | Pulsar 可选的配置集，格式为 `properties.key='value'`。有关详细信息，参见[配置参数](#配置参数)。 | 否   |
| key.format                    | null          | Pulsar 消息的键序列化格式。支持 `raw`、`avro`、`json` 等格式。       | 否   |
| key.fields                    | null          | 序列化键时需要使用的 SQL 定义字段。如有多个字段，使用半角逗号（,）连接。   | 否   |
| key.fields-prefix             | null          | 为键格式的所有字段定义一个自定义前缀，以避免名称与值格式的字段冲突。默认情况下，前缀为空。如果定义了自定义前缀，则 Table 模式和 `'key.fields'` 都将使用带前缀的名称。构造密钥格式的数据类型时，前缀将被删除，并且密钥格式内使用非前缀名称。 | 否   |
| format或value.format          | null          | Pulsar 消息正文的序列化格式。支持 `json`、`avro` 等格式。有关详细信息，参见 Flink 格式 | 是   |
| value.fields-include          | ALL           | Pulsar 消息正文包含的字段策略。支持 `ALL` 和 `EXCEPT_KEY` 选项。           | 否   |

#### Pulsar 消息的元数据配置

`METADATA` 标志用于读写 Pulsar 消息中的元数据，如下所示

> **说明**  
> R/W 列定义了元数据字段是否可读（R）和/或可写（W）。只读列必须声明为 VIRTUAL，以便在 INSERT INTO 操作中排除它们。**

|    元数据   | 数据类型 | 描述 | R/W  |
| ---- | --------- | ----------- | ---- |
| topic | STRING NOT NULL | Pulsar 消息所在的 topic 的名称。 | R    |
| messageId | BYTES NOT NULL | Pulsar 消息 ID。 |R |
| sequenceId | BIGINT NOT NULL| Pulsar 消息的序列号。 | R |
| publishTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Pulsar 消息的发布时间。 | R |
| eventTime | TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL | Pulsar 消息的生成时间。 |R/W |
| properties | MAP<STRING, STRING> NOT NULL | Pulsar 消息的扩展信息。 | R/W |

### Catalog

Flink 始终在当前目录和数据库中搜索表，视图和 UDF。如需使用 Pulsar Catalog 并将 Pulsar 中的 Topic 用作 Flink 中的表，用户应该使用已在 `./conf/sql-client-defaults.yaml` 中定义的 `pulsarcatalog`。

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

以下配置在环境文件中是可选的，或者可以使用 `SET` 命令在 SQL 客户端会话中覆盖原先的取值。

<table class="table">
<tr><th>Option</th><th>Value</th><th>Default</th><th>Description</th></tr>
<tr>
  <td>`default-database`</td>
  <td>默认数据库名称。</td>
  <td>public/default</td>
  <td>使用 Pulsar catalog 时，Pulsar 中的 Topic 用作 Flink 中的表。因此，`database` 是 `tenant/namespace` 的另一个名称。数据库是表查找或创建的基本路径。</td>
  </tr>
  <tr>
  <td>`table-default-partitions`</td>
  <td>Topic 默认分区</td>
  <td>5</td>
  <td>使用 Pulsar catalog 时，Pulsar 中的 Topic 用作 Flink 中的表。因此，创建 Topic 时，需要设置 Topic 分区大小。</td>
  </tr>
</table>

更多详细信息，参见[DDL 配置](#ddl-配置)。

> **说明**  
> Catalog 不支持删除`tenant/namespace`、`topic`。

# 高级特性

本章节介绍 Pulsar Flink 连接器支持的高级特性。

## Pulsar 原生类型

Pulsar 自身提供了一些基本的原生类型。用户可以通过以下方式使用 Pulsar 的原生类型。

### Stream API 环境

PulsarPrimitiveSchema 是 `PulsarDeserializationSchema` 和 `PulsarSerializationSchema` 接口的实现形式。用户可以采用类似 `new PulsarSerializationSchema(String.class)` 的方法创所需要的实例。

### Table 环境

我们创建了一款新的 Flink format 组件，命名为`atomic`。用户可以在 SQL 的 format 中使用。在 Source 中，它会将 Pulsar 原生类型翻译成只有一列值的 RowData。在Sink 中，它会将 RowData 的第一列翻译成 Pulsar 原生类型并写入 Pulsar。

## Upsert Pulsar

Flink 社区用户对 Upsert 模式消息队列有很高的需求，主要原因有三个：

- 将 Pulsar Topic 解释为一个 changelog 流，它将带有键的记录解释为 upsert 事件；
- 作为实时管道的一部分，将多个流连接起来进行充实，并将结果存储在 Pulsar Topic 中，以便进一步的计算。但结果可能包含更新事件。
- 作为实时管道的一部分，聚合数据流并将结果存储在 Pulsar Topic 中，以便进一步计算。但是结果可能包含更新事件。

基于这些需求，我们也实现了对 Upsert Pulsar 的支持。该功能支持用户以 upsert 的方式从 Pulsar Topic 中读取数据和向 Pulsar Topic 中写入数据。

在 SQL DDL 定义中，用户将 `connector` 设置为 `upsert-pulsar`，即可使用 Upsert Pulsar 连接器。

**在配置方面，必须指定 Table 的主键，且不能使用 `key.fields`、`key.fields-prefix`。**

作为 source，Upsert Pulsar 连接器生产 changelog 流，其中每条数据记录代表一个更新或删除事件。更准确地说，如果存在这个 key（，数据记录中的 value 是同一键的最后一个值的 UPDATE。如果不存在相应的 key，则该更新被视为 INSERT）。用表来类比，changelog 流中的数据记录是 UPSERT，也称为 INSERT/UPDATE，因为任何具有相同键的已存在行都会被覆盖。另外，值为空的消息将会被视作为 DELETE 消息。

作为 sink，Upsert Pulsar 连接器可以消费 changelog 流。它会将 INSERT/UPDATE_AFTER 数据作为正常的 Pulsar 消息写入，并将 DELETE 数据作为 value 为空的 Pulsar 消息写入（表示对应键的消息被删除）。Flink 将根据主键列的值对数据进行分区，从而保证主键上的消息有序，因此同一主键上的更新/删除消息将落在同一分区中。

## Key-Shared 订阅模式

在有些场景下，用户需要严格保证消息顺序，这样才能保证正确处理业务。通常，在消息严格保序的情况下，只能同时有一个 consumer 消费消息。这样会导致消息的吞吐量大幅度降低。Pulsar 为这样的场景设计了 Key-Shared 订阅模式。在该模式下，为消息增加 Key，将相同 Key Hash 的消息路由到同一个 consumer ，这样既保证了消息的消息顺序，又提高了吞吐量。

Pulsar Flink 连接器也支持 Key-Shared 订阅模式。可以通过配置参数 `enable-key-hash-range=true` 启用这个功能。使能后，系统会根据任务的并行度划分每个 consumer 处理的 Key Hash 范围。

## 配置参数

该参数对应StreamAPI中的FlinkPulsarSource、FlinkPulsarSink构造参数中的Properties对象，Table模式下的配置properties参数。

| 参数                                 | 默认值        | 描述                                                         | 生效范围     |
| ------------------------------------ | ------------- | ------------------------------------------------------------ | ------------ |
| topic                                | null          | Pulsar Topic。                                                 | source       |
| topics                               | null          | 使用半角逗号（,）连接的多个 Pulsar Topic。                              | source       |
| topicspattern                        | null          | 使用 Java 正则匹配多的多个 pulsar Topic。                             | source       |
| partition.discovery.interval-millis  | -1            | 自动发现增减 Topic，单位为毫秒。取值为-1，则表示禁用该功能。                      | source       |
| clientcachesize                      | 100           | Pulsar 客户端的缓存数量。                                       | source、sink |
| auth-plugin-classname                | null          | Pulsar 客户端的鉴权类。                                       | source、sink |
| auth-params                          | null          | Pulsar 客户端的鉴权参数。                                      | source、sink |
| flushoncheckpoint                    | true          | 在 Flink snapshotState 时，向 Pulsar Topic 中写入消息。                      | sink         |
| failonwrite                          | false         | Sink 出错时，继续确认消息。                                   | sink         |
| polltimeoutms                        | 120000        | 等待获取下一条消息的超时时间，单位为毫秒。                        | source       |
| pulsar.reader.fail-on-data-loss      | true          | 数据丢失时，是否失败。                                       | source       |
| pulsar.reader.use-earliest-when-data-loss | false | 数据丢失时，使用earliest重置offset。 | source |
| commitmaxretries                     | 3             | 向 Pulsar 消息偏移 offset 时，最大重试次数。                    | source       |
| send-delay-millisecond               | 0             | 延迟消息发送(毫秒),仅限于TableApi,StreamApi请参考`PulsarSerializationSchema.setDeliverAtExtractor`           | Sink         |
| scan.startup.mode                    | latest        | 消费消息的位置。支持 `earliest` 和 `latest`选项。                      | source       |
| enable-key-hash-range                | false         | 开启 Pulsar Key-Shared 订阅模式。                                    | source       |
| pulsar.reader.*                      |               | Pulsar reader 的详细配置。有关详细信息，参见 [Pulsar Reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader)。 | source       |
| pulsar.reader.subscriptionRolePrefix | flink-pulsar- | 未指定订阅者时，自动创建订阅者名称的前缀。                     | source       |
| pulsar.reader.receiverQueueSize      | 1000          | 接收队列大小。                                                 | source       |
| pulsar.producer.*                    |               | Pulsar producer 的详细配置。有关详细信息，参见 [Pulsar Producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer)。 | Sink         |
| pulsar.producer.sendTimeoutMs        | 30000         | 发送消息时的超时时间，单位为毫秒。                                 | Sink         |
| pulsar.producer.blockIfQueueFull     | false         | Producer 写入消息的队列满时，支持阻塞方法，而不是抛出异常。           | Sink         |

`pulsar.reader.*` 和 `pulsar.producer.*` 定义配置 Pulsar 行为的详细信息。星号（*）可以替换为 Pulsar 中的配置名，有关详细信息，参见 [Pulsar Reader](https://pulsar.apache.org/docs/en/client-libraries-java/#reader)和 [Pulsar Producer](https://pulsar.apache.org/docs/en/client-libraries-java/#producer)。

在 DDL 语句中，用户可以采用以下配置:

```
'properties.pulsar.reader.subscriptionRolePrefix' = 'pulsar-flink-',
'properties.pulsar.producer.sendTimeoutMs' = '30000',
```

## 认证配置

对于配置了认证的 Pulsar 实例，可以使用与常规 Pulsar 客户端相类似的方式设置 Pulsar Flink 连接器。

1. 对于 `FlinkPulsarSource` 和 `FlinkPulsarSink`，支持通过以下两种方式设置认证。

   - 构造参数 `Properties` 参数。

     ```java
     props.setProperty(PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY, "org.apache.pulsar.client.impl.auth.AuthenticationToken");
     props.setProperty(PulsarOptions.AUTH_PARAMS_KEY, "token:eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyMSJ9.2AgtxHe8-2QBV529B5DrRtpuqP6RJjrk21Mhnomfivo");
     ```

   - 构造参数 `ClientConfigurationData` 参数。`ClientConfigurationData` 参数的优先级高于 `Properties` 参数的优先级。

     ```java
     ClientConfigurationData conf = new ClientConfigurationData();
     conf.setServiceUrl(serviceUrl);
     conf.setAuthPluginClassName(className);
     conf.setAuthParams(params);
     ```

2. 对于通过 SQL 或者 Table 使用 Pulsar 认证，需要设置 `properties.auth-plugin-classname`、`properties.auth-params` 参数。

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
       'scan.startup.mode' = 'earliest',
       'properties.auth-plugin-classname' = 'org.apache.pulsar.client.impl.auth.AuthenticationToken',
       'properties.auth-params' = 'token:xxxxxxxxxx',
   )
   ```

有关认证的详细信息，参见 [Pulsar Security](https://pulsar.apache.org/docs/en/security-overview/)。

## ProtoBuf 支持【试验特性】

该功能基于[Flink: New Format of protobuf](https://github.com/apache/flink/pull/14376),目前正处于等待合并中。
该功能在SQL模式使用如下: 

```
create table pulsar (
                        a INT,
                        b BIGINT,
                        c BOOLEAN,
                        d FLOAT,
                        e DOUBLE,
                        f VARCHAR(32),
                        g BYTES,
                        h VARCHAR(32),
                        f_abc_7d INT,
                        `eventTime` TIMESTAMP(3) METADATA,
                        compute as a + 1,
                        watermark for eventTime as eventTime
                        ) with (
                        'connector' = 'pulsar',
                        'topic' = 'test-protobuf',
                        'service-url' = 'pulsar://localhost:6650',
                        'admin-url' = 'http://localhost:8080',
                        'scan.startup.mode' = 'earliest',
                        'format' = 'protobuf',
                        'protobuf.message-class-name' = 'org.apache.flink.formats.protobuf.testproto.SimpleTest'
                        )

INSERT INTO pulsar VALUES (1,2,false,0.1,0.01,'haha', ENCODE('1', 'utf-8'), 'IMAGES',1, TIMESTAMP '2020-03-08 13:12:11.123');
```

要求：`SimpleTest`类必须实现`GeneratedMessageV3`。
由于该Flink Format: ProtoBuf组件未合并，暂放于本仓库一份源代码用于打包和依赖。
