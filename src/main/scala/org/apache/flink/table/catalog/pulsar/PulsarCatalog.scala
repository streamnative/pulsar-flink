/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.catalog.pulsar

import java.{util => ju}
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.flink.pulsar.{PulsarMetadataReader, PulsarOptions}
import org.apache.flink.streaming.connectors.pulsar.PulsarTableSourceSinkFactory
import org.apache.flink.table.catalog.{AbstractCatalog, CatalogBaseTable, CatalogDatabase, CatalogDatabaseImpl, CatalogFunction, CatalogPartition, CatalogPartitionSpec, CatalogTableImpl, ObjectPath}
import org.apache.flink.table.catalog.stats.{CatalogColumnStatistics, CatalogTableStatistics}
import org.apache.flink.table.factories.TableFactory

class PulsarCatalog(
    adminUrl: String,
    catalogName: String,
    properties: ju.Map[String, String],
    defaultDatabase: String = "public/default")
  extends AbstractCatalog(catalogName, defaultDatabase) {

  override def getTableFactory: Optional[TableFactory] = {
    Optional.of(new PulsarTableSourceSinkFactory)
  }

  private var metadataReader: PulsarMetadataReader = null

  override def open(): Unit = {
    if (metadataReader == null) {
      metadataReader = PulsarMetadataReader(adminUrl, null, "", Map.empty[String, String])
    }
  }

  override def close(): Unit = {
    if (metadataReader != null) {
      metadataReader.close()
    }
    metadataReader = null
  }

  override def listDatabases(): ju.List[String] = {
    metadataReader.listNamespaces().asJava
  }

  override def getDatabase(databaseName: String): CatalogDatabase = {
    val properties = new ju.HashMap[String, String]()
    new CatalogDatabaseImpl(properties, databaseName)
  }

  override def databaseExists(s: String): Boolean = {
    metadataReader.namespaceExists(s)
  }

  override def createDatabase(
      databaseName: String, database: CatalogDatabase, b: Boolean): Unit = {
    metadataReader.createNamespace(databaseName)
  }

  override def dropDatabase(name: String, ignoreIfNotExists: Boolean): Unit = {
    metadataReader.deleteNamespace(name, ignoreIfNotExists)
  }

  override def listTables(databaseName: String): ju.List[String] = {
    metadataReader.getTopics(databaseName)
  }

  override def getTable(objectPath: ObjectPath): CatalogBaseTable = {
    new CatalogTableImpl(
      metadataReader.getSchema(objectPath),
      properties, "")
  }

  override def tableExists(objectPath: ObjectPath): Boolean = {
    metadataReader.topicExists(objectPath)
  }

  override def dropTable(objectPath: ObjectPath, b: Boolean): Unit = {
    metadataReader.deleteTopic(objectPath)
  }

  override def createTable(
    tablePath: ObjectPath,
    table: CatalogBaseTable,
    ignoreIfExists: Boolean): Unit = {

    val defaultNumPartitions = table.getProperties.get(PulsarOptions.NUM_PARTITIONS).toInt
    val defaultNumPartitions2 = properties.get(PulsarOptions.NUM_PARTITIONS).toInt

    metadataReader.createTopic(tablePath, defaultNumPartitions, ignoreIfExists)
  }

  // ------------------------------------------------------------------------
  // Unsupported catalog operations for Pulsar
  // ------------------------------------------------------------------------

  override def alterDatabase(s: String, catalogDatabase: CatalogDatabase, b: Boolean): Unit =
    throw new UnsupportedOperationException

  override def listViews(s: String): ju.List[String] = throw new UnsupportedOperationException

  override def alterTable(
    objectPath: ObjectPath,
    catalogBaseTable: CatalogBaseTable,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def renameTable(objectPath: ObjectPath, s: String, b: Boolean): Unit =
    throw new UnsupportedOperationException

  override def listPartitions(objectPath: ObjectPath): ju.List[CatalogPartitionSpec] =
    throw new UnsupportedOperationException

  override def listPartitions(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec): ju.List[CatalogPartitionSpec] =
    throw new UnsupportedOperationException

  override def getPartition(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec): CatalogPartition =
    throw new UnsupportedOperationException

  override def partitionExists(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec): Boolean = throw new UnsupportedOperationException

  override def createPartition(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec,
    catalogPartition: CatalogPartition,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def dropPartition(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def alterPartition(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec,
    catalogPartition: CatalogPartition,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def listFunctions(
    s: String): ju.List[String] = throw new UnsupportedOperationException

  override def getFunction(
    objectPath: ObjectPath): CatalogFunction = throw new UnsupportedOperationException

  override def functionExists(
    objectPath: ObjectPath): Boolean = throw new UnsupportedOperationException

  override def createFunction(
    objectPath: ObjectPath,
    catalogFunction: CatalogFunction, b: Boolean): Unit = throw new UnsupportedOperationException

  override def alterFunction(
    objectPath: ObjectPath,
    catalogFunction: CatalogFunction, b: Boolean): Unit = throw new UnsupportedOperationException

  override def dropFunction(
    objectPath: ObjectPath, b: Boolean): Unit = throw new UnsupportedOperationException

  override def getTableStatistics(
    objectPath: ObjectPath): CatalogTableStatistics = throw new UnsupportedOperationException

  override def getTableColumnStatistics(
    objectPath: ObjectPath): CatalogColumnStatistics = throw new UnsupportedOperationException

  override def getPartitionStatistics(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec): CatalogTableStatistics =
    throw new UnsupportedOperationException

  override def getPartitionColumnStatistics(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec): CatalogColumnStatistics =
    throw new UnsupportedOperationException

  override def alterTableStatistics(
    objectPath: ObjectPath,
    catalogTableStatistics: CatalogTableStatistics,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def alterTableColumnStatistics(
    objectPath: ObjectPath,
    catalogColumnStatistics: CatalogColumnStatistics,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def alterPartitionStatistics(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec,
    catalogTableStatistics: CatalogTableStatistics,
    b: Boolean): Unit = throw new UnsupportedOperationException

  override def alterPartitionColumnStatistics(
    objectPath: ObjectPath,
    catalogPartitionSpec: CatalogPartitionSpec,
    catalogColumnStatistics: CatalogColumnStatistics,
    b: Boolean): Unit = throw new UnsupportedOperationException
}
