package com.thinkaurelius.titan.graphdb.secure;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.indexing.IndexProvider;
import com.thinkaurelius.titan.diskstorage.indexing.IndexTransaction;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.BufferTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction;
import com.thinkaurelius.titan.graphdb.transaction.TransactionConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.Configuration;

/**
 *
 * @author edeprit@42six.com
 * @param <T>
 */
public class SecureBackend<T extends SecurityToken> extends Backend {

    public SecureBackend(Configuration storageConfig) {
        super(storageConfig);
    }

    public SecureBackend(Configuration storageConfig, Configuration metricsConfig) {
        super(storageConfig, metricsConfig);
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in
     * one {@link BackendTransaction}.
     *
     * @param configuration
     * @param indexKeyRetriever
     * @return
     * @throws StorageException
     */
    @Override
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws StorageException {
        SecureStoreTxConfig<T> txConfig = new SecureStoreTxConfig<T>(configuration.getMetricsPrefix());
        if (configuration.hasTimestamp()) {
            txConfig.setTimestamp(configuration.getTimestamp());
        }

        if (configuration instanceof SecureTransactionConfiguration) {
            SecureTransactionConfiguration<T> secureConfig = (SecureTransactionConfiguration<T>) configuration;
            if (secureConfig.hasReadToken()) {
                txConfig.setReadToken(secureConfig.getReadToken());
            }
            if (secureConfig.hasWriteToken()) {
                txConfig.setWriteToken(secureConfig.getWriteToken());
            }
        }

        StoreTransaction tx = storeManager.beginTransaction(txConfig);
        if (bufferSize > 1) {
            Preconditions.checkArgument(storeManager.getFeatures().supportsBatchMutation());
            tx = new BufferTransaction(tx, storeManager, bufferSize, writeAttempts, persistAttemptWaittime);
        }
        if (!storeFeatures.supportsLocking()) {
            if (storeFeatures.supportsTransactions()) {
                //No transaction wrapping needed
            } else if (storeFeatures.supportsConsistentKeyOperations()) {
                txConfig = new SecureStoreTxConfig(ConsistencyLevel.KEY_CONSISTENT, configuration.getMetricsPrefix());
                if (configuration.hasTimestamp()) {
                    txConfig.setTimestamp(configuration.getTimestamp());
                }
                tx = new ExpectedValueCheckingTransaction(tx,
                        storeManager.beginTransaction(txConfig),
                        readAttempts);
            }
        }

        //Index transactions
        Map<String, IndexTransaction> indexTx = new HashMap<String, IndexTransaction>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            SecureIndexTransaction stxh = new SecureIndexTransaction<T>(entry.getValue(), indexKeyRetriever.get(entry.getKey()));

            if (configuration instanceof SecureTransactionConfiguration) {
                SecureTransactionConfiguration<T> secureConfig = (SecureTransactionConfiguration<T>) configuration;

                if (secureConfig.hasReadToken()) {
                    stxh.setReadToken(secureConfig.getReadToken());
                }
                if (secureConfig.hasWriteToken()) {
                    stxh.setWriteToken(secureConfig.getWriteToken());
                }
            }

            indexTx.put(entry.getKey(), stxh);
        }

        return new BackendTransaction(tx, storeManager.getFeatures(),
                edgeStore, vertexIndexStore, edgeIndexStore,
                readAttempts, persistAttemptWaittime, indexTx, threadPool);
    }
}
