# pulsar-flink
Pulsar Flink connector实现了使用 [Apache Pulsar](https://pulsar.apache.org) 和 [Apache Flink](https://flink.apache.org)进行弹性数据处理的功能。

## 前提条件

- Java 8或更高
- Flink 1.9.0 或更高
- Pulsar 2.4.0 或更高

## 基本

### Link

#### 客户端  
对于使用SBT、Maven、Gradle的项目，可以使用下面参数设置到您的项目。

- `FLINK_VERSION`参数现在有`1.9.0`和`1.11.1`可选。
  - 1.9.0版本支持flink 1.9-1.10
  - 1.11.1版本支持1.11+
- `SCALA_BINARY_VERSION`参数很flink使用的scala版本有关，现有`2.11`、`2.12`可选。
- `PULSAR_FLINK_VERSION`是本连接器的版本。

```
    groupId = io.streamnative.connectors
    artifactId = pulsar-flink-{{SCALA_BINARY_VERSION}}-{{FLINK_VERSION}}
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





### 部署

#### Client library  
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

#### Scala REPL  
在交互式的Scala shell中进行尝试 `bin/start-scala-shell.sh`，你可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

栗子🌰

```
$ ./bin/start-scala-shell.sh remote <hostname> <portnumber>
  --addclasspath pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
有关使用CLI提交应用程序的更多信息，请参考 [Command-Line Interface](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/cli.html).


#### SQL Client
要使用 [SQL Client Beta](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sqlClient.html) 并编写SQL查询操作Pulsar中的数据，你可以使用 `--addclasspath` 参数直接添加 `pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar`。

栗子🌰
```
$ ./bin/sql-client.sh embedded --jar pulsar-flink-connector_{{SCALA_BINARY_VERSION}}-{{PULSAR_FLINK_VERSION}}.jar
```
默认情况下，要在SQL Client中使用Pulsar目录并在启动时自动进行注册，SQL Client会从环境文件`./conf/sql-client-defaults.yaml`中读取其配置。 您需要在此YAML文件的`catalogs`部分中添加Pulsar目录：

```yaml
catalogs:
- name: pulsarcatalog
    type: pulsar
    default-database: tn/ns
    service-url: "pulsar://localhost:6650"
    admin-url: "http://localhost:8080"
```



## Stream环境

### Source

Flink的Pulsar消费者称为`FlinkPulsarSource<T>`或仅具有数据模式自动推断功能的`FlinkPulsarRowSource`。 它提供对一个或多个Pulsar主题的访问。

构造方法有以下参数：

1. 连接Pulsar实例使用的服务地址`serviceUrl`和管理地址`adminUrl`。
2. 使用 `FlinkPulsarSource`时，需要设`DeserializationSchema<T>`或者`PulsarDeserializationSchema<T>`。
3. Properties参数，用于配置Pulsar Consumer的行为。
   Properties必须的参数，如下:

    - 这些参数中 `topic`, `topics` or `topicsPattern` 必须有一个存在值，且只能有一个。用于配置消费的Topic信息. (**`topics` 是用逗号`,`分隔的多个topic, `topicsPattern`是一个java正则表达式，可以匹配出若干topic**)
   
      设置消费模式可以通过 FlinkPulsarSource的setStartFromLatest、setStartFromEarliest、setStartFromSpecificOffsets、setStartFromSubscription等设置。


例子:

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
Properties props = new Properties();
props.setProperty("topic", "test-source-topic");
props.setProperty("partitiondiscoveryintervalmillis", "5000");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(serviceUrl, adminUrl, new SimpleStringSchema(), props);

// or setStartFromLatest、setStartFromSpecificOffsets、setStartFromSubscription
source.setStartFromEarliest(); 

DataStream<String> stream = see.addSource(source);

// chain operations on dataStream of String and sink the output
// end method chaining

see.execute();
```



#### Pulsar Source和时间戳提取/水印Emission

在许多情况下，记录的时间戳（显式或隐式地）嵌入到记录本身中。另外，用户可能想要周期性地或以不规则的方式发出水印。基于包含当前事件时间水印的Pulsar流中的特殊记录。对于这些情况，Flink Pulsar源允许指定`AssignerWithPeriodicWatermarks`或` AssignerWithPunctuatedWatermarks`。

在内部，每个Pulsar分区都执行分配器的实例。指定了这样的分配器后，对于从Pulsar读取的每个记录，调用`extractTimestamp（T element，long previousElementTimestamp）`为记录分配时间戳，然后`Watermark getCurrentWatermark()`（用于定期）或调用水印`checkAndGetNextWatermark(T lastElement，long extractTimestamp)`（用于标点符号）确定是否应该发出新的水印以及使用哪个时间戳。

### Sink

使用 `FlinkPulsarSink`实例处理POJO类型或 `FlinkPulsarRowSink` 处理Flink Row类型.
它允许将记录流写入一个或多个Pulsar topic。

Example:

```java
FlinkPulsarSink<Person> sink = new FlinkPulsarSink(
  serviceUrl,
  adminUrl,
  Optional.of(topic),      // mandatory target topic or use `Optional.empty()` if sink to different topics for each record
  props,
  TopicKeyExtractor.NULL,  // replace this to extract key or topic for each record
  Person.class,
  RecordSchemaType.AVRO);

stream.addSink(sink);
```

如果记录record中包含了topic信息，可已通过自定义TopicKeyExtractor实现，来让消息分发到不同队列。



#### 容错

启用Flink的检查点后，`FlinkPulsarSink`和`FlinkPulsarRowSink`可以提供at-least-once的交货保证。

除了启用Flink的检查点之外，您还应该配置`setLogFailuresOnly(boolean)` 和 `setFlushOnCheckpoint(boolean)`。

  *`setFlushOnCheckpoint(boolean)`*：默认情况下，它设置为`true`。启用此功能后，写入pulsar记录会在本次checkpoint snapshotState时执行。这样可以确保checkpoint之前的所有记录写给pulsar了。注意必须同时开启flink的at-least-once设置。

## Table环境



Pulsar-flink连接器对于flink的Table支持的很全面，覆盖了以下列表：

- Connector
- Catalog
- SQL、DDL（DDL在flink 1.11支持）



### Connector

```java
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
see.setParallelism(1);
StreamTableEnvironment tEnv = StreamTableEnvironment.create(see);

String topic = "";
String tableName = "pulsarTable";

TableSchema tSchema = TableSchema.builder().field("value", DataTypes.BOOLEAN()).build();
tEnv.connect(
  			new Pulsar()
                .urls(getServiceUrl(), getAdminUrl())
                .topic(topic)
                .startFromEarliest()
                .property(PulsarOptions.PARTITION_DISCOVERY_INTERVAL_MS_OPTION_KEY, "5000")
				)
        .withSchema(new Schema().schema(tSchema))
        .inAppendMode()
        .createTemporaryTable(tableName);
Table t = tEnv.sqlQuery("select `value` from " + tableName);
```

### Catalog

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


注意：由于删除操作具有危险性，catalog中删除`tenant/namespace`、`Flink`操作不被支持。



### SQL、DDL



```sql
set global.disable.operator.chain = true;

create table test_flink_sql(
	`rip` VARCHAR,
	`rtime`	VARCHAR,
	`uid`	bigint,
  `rchannel`	VARCHAR,
  `be_time`	bigint,
  `be_time`	VARCHAR,
	`activity_id` VARCHAR,
	`country_code`	VARCHAR,
	`os`	VARCHAR,
	`recv_time`	bigint,
	`remark`	VARCHAR,
	`client_ip`	VARCHAR,
	`day` as TO_DATE(rtime),
	`hour` as date_format(rtime, 'HH')
) with (
	'connector.type' = 'pulsar',
	'connector.version' = '1',
	'connector.topic' = 'persistent://test/test-gray/test_flink_sql',
  'connector.service-url' = 'pulsar://xxx',
  'connector.admin-url' = 'http://xxx',
	'connector.startup-mode' = 'external-subscription',
	'connector.sub-name' = 'test_flink_sql_v1',
	'connector.properties.0.key' = 'pulsar.reader.readerName',
	'connector.properties.0.value' = 'test_flink_sql_v1',
	'connector.properties.1.key' = 'pulsar.reader.subscriptionRolePrefix',
	'connector.properties.1.value' = 'test_flink_sql_v1',
	'connector.properties.2.key' = 'pulsar.reader.receiverQueueSize',
	'connector.properties.2.value' = '1000',
	'connector.properties.3.key' = 'partitiondiscoveryintervalmillis',
	'connector.properties.3.value' = '5000',
	'format.type' = 'json',
	'format.derive-schema' = 'true',
	'format.ignore-parse-errors' = 'true',
  'update-mode' = 'append'
);

insert into hive.test.test_flink_sql
select
rip, rtime, 
if (uid is null, 0, uid) as uid,
if (activity_id is null, '', activity_id) as activity_id,
if (country_code is null, '', country_code) as country_code,
if (os is null, '', os) as os,
if (recv_time is null, 0, recv_time) as recv_time,
if (remark is null, '', remark) as remark,
if (client_ip is null, '', client_ip) as client_ip,
cast(`day` as string) as `day`, 
cast(`hour` as string) as `hour`
from test_flink_sql;
```



DDL在flink 1.11包中被支持，可以在创建表中，设置更详细的参数。



## DeserializationSchema

DeserializationSchema是用于Source的记录解码，核心方法只能做pulsar Message#value解码。在自定义场景下，用户需要从Message中获得更多信息，就无法满足。

pulsar-flink连接器没有直接使用`DeserializationSchema`,而是定义了`PulsarDeserializationSchema<T>`。通过`PulsarDeserializationSchema<T>`实例，给用户留下了更多的拓展空间。

使用`new PulsarDeserializationSchemaWrapper<>(deserializationSchema)`可支持`DeserializationSchema`的实例。

pulsar-flink连接器连接器提供了了两款`DeserializationSchema`解码器：

1. `JsonDeser`: pulsar topic 使用 JSONSchema时, 你可以通过 `JsonDeser.of(POJO_CLASS_NAME.class)` 创建 `DeserializationSchema`实例.

2. `AvroDeser`: pulsar topic 使用 AVROSchema时, 你可以通过 `AvroDeser.of(POJO_CLASS_NAME.class)` for `DeserializationSchema`实例.



### Row类型的自动化解码PulsarDeserializer

Flink 1.9升级到1.11之后，有了较大的变化，很多API不兼容。在两个版本下，对于schema的处理有所差异。

在flink 1.9时，创建一个table时，配置schema参数是可选的。但升级到flink1.11时，schema是必须指定的，并且必须与TableSource返回类型一致。

**这影响了`PulsarDeserializer`对flink row类型解码**，并产生了两个差异：

1. 拓展字段类型差异：

   | Column          | flink 1.9中Type | flink 1.11中Type |
   | --------------- | --------------- | ---------------- |
   | `__key`         | Bytes           | Bytes            |
   | `__topic`       | String          | String           |
   | `__messageId`   | Bytes           | Bytes            |
   | `__publishTime` | Timestamp       | LocalDateTime    |
   | `__eventTime`   | Timestamp       | LocalDateTime    |

   

   

2. 拓展字段配置:

   - flink 1.9 拓展字段会默认添加
   - flink 1.11 默认不使用拓展字段，配置`use-extend-field=true`时开启，且需要在schema上声明拓展字段。catalog模式下默认开启。



## 高级配置

### 配置参数



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

在DDL语句中，使用上述参数的格式，有所调整，

配置`pulsar.reader.readerName=test_flink_sql_v1`的设置

```
'connector.properties.0.key' = 'pulsar.reader.readerName', //参数名
'connector.properties.0.value' = 'test_flink_sql_v1',      // 参数值
```

Example：

```sql
create table test_flink_sql(
	`data` VARCHAR
) with (
	'connector.type' = 'pulsar',
	'connector.version' = '1',
	'connector.topic' = 'persistent://test/test-gray/test_flink_sql',
  'connector.service-url' = 'pulsar://xxx',
  'connector.admin-url' = 'http://xxx',
	'connector.startup-mode' = 'earliest',  //订阅模式
	'connector.properties.0.key' = 'pulsar.reader.readerName', //参数名
	'connector.properties.0.value' = 'test_flink_sql_v1',      // 参数值
	'connector.properties.1.key' = 'pulsar.reader.subscriptionRolePrefix',
	'connector.properties.1.value' = 'test_flink_sql_v1',
	'connector.properties.2.key' = 'pulsar.reader.receiverQueueSize',
	'connector.properties.2.value' = '1000',
	'connector.properties.3.key' = 'partitiondiscoveryintervalmillis', //参数名
	'connector.properties.3.value' = '5000',                           //参数值
  'update-mode' = 'append'
);
```



### 身份验证配置

对于配置了身份验证的Pulsar实例，可以使用常规Pulsar客户端类似的方式设置Pulsar Flink连接器。

对于FlinkPulsarSource，FlinkPulsarRowSource，FlinkPulsarSink和FlinkPulsarRowSink，它们都带有构造函数，使您能够
传入`ClientConfigurationData`作为参数之一。 您应该先构造一个`ClientConfigurationData`，然后将其传递给相应的构造函数。

For example:

```java
ClientConfigurationData conf = new ClientConfigurationData();
conf.setServiceUrl(serviceUrl);
conf.setAuthPluginClassName(className);
conf.setAuthParams(params);

Properties props = new Properties();
props.setProperty("topic", "test-source-topic");

FlinkPulsarSource<String> source = new FlinkPulsarSource<>(adminUrl, conf, new SimpleStringSchema(), props);

```

 

## 构建 Pulsar Flink Connector
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

