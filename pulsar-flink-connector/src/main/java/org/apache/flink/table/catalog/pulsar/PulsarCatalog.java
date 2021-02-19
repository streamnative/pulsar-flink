/*
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

package org.apache.flink.table.catalog.pulsar;

import org.apache.flink.streaming.connectors.pulsar.internal.IncompatibleSchemaException;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarCatalogSupport;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.SimpleSchemaTranslator;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarDynamicTableFactory;
import org.apache.flink.streaming.connectors.pulsar.table.PulsarTableOptions;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * Expose a Pulsar instance as a database catalog.
 */
@Slf4j
public class PulsarCatalog extends GenericInMemoryCatalog {

    private String adminUrl;

    private Map<String, String> properties;

    private PulsarCatalogSupport catalogSupport;

    public PulsarCatalog(String adminUrl, String catalogName, Map<String, String> props, String defaultDatabase) {
        super(catalogName, defaultDatabase);
        this.adminUrl = adminUrl;
        this.properties = new HashMap<>(props);
        log.info("Created Pulsar Catalog {}", catalogName);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new PulsarDynamicTableFactory(true));
    }

    @Override
    public void open() throws CatalogException {
        if (catalogSupport == null) {
            try {
                final ClientConfigurationData clientConf = new ClientConfigurationData();
                clientConf.setAuthParams(properties.get("properties." + PulsarOptions.AUTH_PARAMS_KEY));
                clientConf.setAuthPluginClassName(
                        properties.get("properties." + PulsarOptions.AUTH_PLUGIN_CLASSNAME_KEY));
                catalogSupport = new PulsarCatalogSupport(adminUrl, clientConf, "",
                        new HashMap<>(), -1, -1, new SimpleSchemaTranslator(false));
            } catch (PulsarClientException e) {
                throw new CatalogException("Failed to create Pulsar admin using " + adminUrl, e);
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        if (catalogSupport != null) {
            catalogSupport.close();
            catalogSupport = null;
            log.info("Close connection to Pulsar");
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return catalogSupport.listNamespaces();
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to list all databases in %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        Map<String, String> properties = new HashMap<>();
        return new CatalogDatabaseImpl(properties, databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return catalogSupport.namespaceExists(databaseName);
        } catch (PulsarAdminException e) {
            return false;
        } catch (Exception e) {
            log.warn("{} database does not exist. {}", databaseName, e.getMessage());
            return false;
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            catalogSupport.createNamespace(name);
        } catch (PulsarAdminException.ConflictException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name, e);
            }
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to create database %s", name), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            return catalogSupport.getTopics(databaseName);
        } catch (PulsarAdminException.NotFoundException e) {
            throw new DatabaseNotExistException(getName(), databaseName, e);
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        try {
            return new CatalogTableImpl(catalogSupport.getTableSchema(tablePath), properties, "");
        } catch (PulsarAdminException.NotFoundException e) {
            throw new TableNotExistException(getName(), tablePath, e);
        } catch (PulsarAdminException | IncompatibleSchemaException e) {
            throw new CatalogException(String.format("Failed to get table %s schema", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return catalogSupport.topicExists(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to check table %s existence", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        final String key = CONNECTOR + "." + PulsarOptions.DEFAULT_PARTITIONS;
        int defaultNumPartitions = Integer.parseInt(properties.getOrDefault(key, "5"));
        String databaseName = tablePath.getDatabaseName();
        Boolean databaseExists;
        try {
            databaseExists = catalogSupport.namespaceExists(databaseName);
        } catch (PulsarAdminException e) {
            throw new CatalogException(String.format("Failed to check existence of databases %s", databaseName), e);
        }

        if (!databaseExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        try {
            catalogSupport.createTopic(tablePath, defaultNumPartitions, table);
            catalogSupport.putSchema(tablePath, table, getFormat());
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 409) {
                throw new TableAlreadyExistException(getName(), tablePath, e);
            } else {
                throw new CatalogException(String.format("Failed to create table %s", tablePath.getFullName()), e);
            }
        } catch (IncompatibleSchemaException e) {
            throw new CatalogException("Failed to translate Flink type to Pulsar", e);
        }
    }

    private String getFormat() {
        return Optional.ofNullable(properties.get(FormatDescriptorValidator.FORMAT))
                .orElseGet(() -> properties.get(PulsarTableOptions.VALUE_FORMAT.key()));
    }

    // ------------------------------------------------------------------------
    // Unsupported catalog operations for Pulsar
    // There should not be such permission in the connector, it is very dangerous
    // ------------------------------------------------------------------------

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> expressions)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
                                boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
            PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
                               boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
                                                                CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
                                     boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
                                           boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                         CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
                                               CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
