package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage Manager for Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloStoreManager.class);
    // Default parameters 
    // private static final Authorizations AUTHORIZATIONS_DEFAULT = new Authorizations();
    // Configuration namespace
    public static final String ACCUMULO_NAMESPACE = "accumulo";
    // Configuration keys
    public static final String ACCUMULO_INSTANCE_KEY = "instance";
    public static final String TABLE_NAME_KEY = "tablename";
    public static final String SERVER_SIDE_ITERATORS_KEY = "server-side-iterators";
    public static final String SPLIT_BITS_KEY = "split-bits";
    // Configuration defaults
    public static final String TABLE_NAME_DEFAULT = "titan";
    public static final int PORT_DEFAULT = 9160;
    public static final boolean SERVER_SIDE_ITERATORS_DEFAULT = false;
    // Instance injector
    protected static AccumuloInstanceFactory instanceFactory = AccumuloInstanceFactory.ZOOKEEPER_INSTANCE_FACTORY;
    // Instance variables
    private final String tableName;
    private final String instanceName;
    private final String zooKeepers;
    private final boolean serverSideIterators;
    private final int splitBits;
    private final Instance instance;    // thread-safe
    private final Connector connector;  // thread-safe
    private final ConcurrentMap<String, AccumuloKeyColumnValueStore> openStores;
    private final StoreFeatures features;   // immutable at constructor exit
    private final BatchWriterConfig batchConfiguration;    // immutable at constructor exit

    private final ObjectPool<BatchWriter> writerPool;

    public AccumuloStoreManager(Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        zooKeepers = config.getString(GraphDatabaseConfiguration.HOSTNAME_KEY,
                GraphDatabaseConfiguration.HOSTNAME_DEFAULT);
        tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);

        // Accumulo specific keys
        Configuration accumuloConfig = config.subset(ACCUMULO_NAMESPACE);
        instanceName = accumuloConfig.getString(ACCUMULO_INSTANCE_KEY);

        serverSideIterators = accumuloConfig.getBoolean(SERVER_SIDE_ITERATORS_KEY, SERVER_SIDE_ITERATORS_DEFAULT);

        splitBits = accumuloConfig.getInt(SPLIT_BITS_KEY, 0);

        instance = instanceFactory.getInstance(instanceName, zooKeepers);

        try {
            connector = instance.getConnector(username, new PasswordToken(password.getBytes()));
        } catch (AccumuloException ex) {
            logger.error("Accumulo failure", ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        } catch (AccumuloSecurityException ex) {
            logger.error("User doesn't have permission to connect", ex);
            throw new PermanentStorageException(ex.getMessage(), ex);
        }

        openStores = new ConcurrentHashMap<String, AccumuloKeyColumnValueStore>();

        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsMultiQuery = true;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = true;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;

        batchConfiguration = new BatchWriterConfig();

        writerPool = new GenericObjectPool<BatchWriter>(new BatchWriterFactory(batchConfiguration, connector, tableName));
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "accumulo[" + getName() + "@" + super.toString() + "]";
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    public Connector getConnector() {
        return connector;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean hasServerSideIterators() {
        return serverSideIterators;
    }

    @Override
    public KeyColumnValueStore openDatabase(String dbName) throws StorageException {
        AccumuloKeyColumnValueStore store = openStores.get(dbName);

        if (store == null) {
            AccumuloKeyColumnValueStore newStore
                    = new AccumuloKeyColumnValueStore(this, dbName);

            store = openStores.putIfAbsent(dbName, newStore); // atomic so only one store dbName

            if (store == null) { // ensure that column family exists on first open
                ensureColumnFamilyExists(dbName);
                store = newStore;
            }
        }

        return store;
    }

    @Override
    public void close() throws StorageException {
        openStores.clear();
        clearWriterPool();
    }

    private void clearWriterPool() throws StorageException {
        if (writerPool.getNumActive() > 0) {
            String msg = "Can't close batch writer pool with active writers";
            logger.error(msg);
            throw new TemporaryStorageException(msg);
        }

        try {
            writerPool.clear();
        } catch (Exception ex) {
            //  Should never catch, GenericObjectPool doesn't throw exception
            logger.info("Error clearing batch writer pool", ex);
            throw new TemporaryStorageException(ex);
        }
    }

    @Override
    public void clearStorage() throws StorageException {
        for (AccumuloKeyColumnValueStore store : openStores.values()) {
            store.close();
        }
        openStores.clear();
        truncateTable();
    }

    @Override
    public StoreTransaction beginTransaction(final StoreTxConfig config) throws StorageException {
        return new AccumuloStoreTransaction(config);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;
        
        AccumuloStoreTransaction atxh = (AccumuloStoreTransaction) txh;

        Collection<Mutation> actions = convertToActions(mutations, putTS, delTS, atxh.getVisibility());

        BatchWriter writer;
        try {
            writer = writerPool.borrowObject();
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        } catch (Exception ex) {
            logger.error("Should not throw this exception", ex);
            throw new PermanentStorageException(ex);
        }

        try {
            writer.addMutations(actions);
            writer.flush();
            try {
                writerPool.returnObject(writer);
            } catch (Exception ex) {
                logger.error("Error returning batch writer to pool", ex);
            }
        } catch (MutationsRejectedException ex) {
            logger.error("Can't write mutations to Titan store " + tableName, ex);
            try {
                writerPool.invalidateObject(writer);
            } catch (Exception poolEx) {
                logger.error("Error returning batch writer to pool", poolEx);
            }
            throw new TemporaryStorageException(ex);
        }

        waitUntil(putTS);
    }

    /**
     * Convert Titan internal {
     *
     * @ KCVMutation} representation into Accumulo native {
     * @ Mutation}.
     *
     * @param mutations Mutations to convert into Accumulo actions.
     * @param putTimestamp The timestamp to use for put mutations.
     * @param delTimestamp The timestamp to use for delete mutations.
     *
     * @return Mutations converted from Titan internal representation.
     */
    private static Collection<Mutation> convertToActions(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
            final long putTimestamp, final long delTimestamp, ColumnVisibility visibility) {

        Map<StaticBuffer, Mutation> actionsPerKey = new HashMap<StaticBuffer, Mutation>();
        
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            Text colFamily = new Text(entry.getKey().getBytes());

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                StaticBuffer key = m.getKey();
                KCVMutation mutation = m.getValue();

                Mutation commands = actionsPerKey.get(key);

                if (commands == null) {
                    commands = new Mutation(new Text(key.as(StaticBuffer.ARRAY_FACTORY)));
                    actionsPerKey.put(key, commands);
                }

                if (mutation.hasDeletions()) {
                    for (StaticBuffer del : mutation.getDeletions()) {
                        commands.putDelete(colFamily, new Text(del.as(StaticBuffer.ARRAY_FACTORY)), visibility, delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry add : mutation.getAdditions()) {
                        commands.put(colFamily, new Text(add.getArrayColumn()), visibility, putTimestamp, new Value(add.getArrayValue()));
                    }
                }
            }
        }

        return actionsPerKey.values();
    }

    private void ensureColumnFamilyExists(String columnFamily) throws StorageException {
        ensureTableExists();

        try {
            TableOperations tableOps = connector.tableOperations();

            Map<String, Set<Text>> localityGroups = tableOps.getLocalityGroups(tableName);
            localityGroups.put(columnFamily, Sets.newHashSet(new Text(columnFamily)));
            tableOps.setLocalityGroups(tableName, localityGroups);
        } catch (AccumuloException ex) {
            logger.error("Accumulo failure", ex);
            throw new PermanentStorageException(ex);
        } catch (AccumuloSecurityException ex) {
            logger.error("User doesn't have permission to create Titan store {}", tableName, ex);
            throw new PermanentStorageException(ex);
        } catch (TableNotFoundException ex) {
            // Should never catch
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }

    private void closeStores() throws StorageException {
        for (KeyColumnValueStore store : openStores.values()) {
            store.close();
        }
        openStores.clear();
    }

    private void truncateTable() throws StorageException {
        try {
            TableOperations tableOps = connector.tableOperations();
            tableOps.delete(tableName);
            tableOps.create(tableName);
        } catch (AccumuloException ex) {
            logger.error("Accumulo failure", ex);
            throw new PermanentStorageException(ex);
        } catch (AccumuloSecurityException ex) {
            logger.error("User doesn't have permission to delete Titan store {}", tableName, ex);
            throw new PermanentStorageException(ex);
        } catch (TableNotFoundException ex) {
            logger.warn("Delete called on non-existent Titan store {}.", tableName);
        } catch (TableExistsException ex) {
            // Should never be caught
            logger.info("Create called on existing Titan store {}.", tableName);
        }
    }

    private void ensureTableExists() throws StorageException {
        try {
            TableOperations operations = connector.tableOperations();

            if (!operations.exists(tableName)) {
                operations.create(tableName);
            }

            if (splitBits > 0) {
                operations.addSplits(tableName, getSplits(splitBits));
            }
        } catch (AccumuloException ex) {
            logger.error("Accumulo failure", ex);
            throw new PermanentStorageException(ex);
        } catch (AccumuloSecurityException ex) {
            logger.error("User doesn't have permission to create Titan store {}", tableName, ex);
            throw new PermanentStorageException(ex);
        } catch (TableExistsException ex) {
            // Concurrent creation of table, this thread lost race
        } catch (TableNotFoundException ex) {
            // Should never catch
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }
    }

    private static SortedSet<Text> getSplits(int splitBits) {
        Preconditions.checkArgument(splitBits <= Byte.SIZE);

        int splitMax = 1 << splitBits;
        int splitOffset = Byte.SIZE - splitBits;

        SortedSet<Text> splits = new TreeSet<Text>();

        byte[] key = new byte[1];
        for (int i = 1; i < splitMax; i++) {
            key[0] = (byte) (i << splitOffset);
            splits.add(new Text(key));
        }
        return splits;
    }

    private static void waitUntil(long until) {
        long now = System.currentTimeMillis();

        while (now <= until) {
            try {
                Thread.sleep(1L);
                now = System.currentTimeMillis();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
