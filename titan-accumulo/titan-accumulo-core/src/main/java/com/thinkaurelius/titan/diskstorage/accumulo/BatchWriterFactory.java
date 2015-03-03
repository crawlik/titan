package com.thinkaurelius.titan.diskstorage.accumulo;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class BatchWriterFactory extends BasePooledObjectFactory<BatchWriter> {

    private static final Logger logger = LoggerFactory.getLogger(BatchWriterFactory.class);
    private final BatchWriterConfig batchConfiguration;
    private final Connector connector;
    private final String tableName;

    public BatchWriterFactory(BatchWriterConfig batchConfiguration, Connector connector, String tableName) {
        this.batchConfiguration = batchConfiguration;
        this.connector = connector;
        this.tableName = tableName;
    }

    @Override
    public BatchWriter create() throws Exception {
        return connector.createBatchWriter(tableName, batchConfiguration);

    }

    @Override
    public PooledObject<BatchWriter> wrap(BatchWriter writer) {
        return new DefaultPooledObject<BatchWriter>(writer);
    }

    @Override
    public void destroyObject(PooledObject<BatchWriter> pooledWriter) throws Exception {
        try {
            pooledWriter.getObject().close();
        } catch (MutationsRejectedException ex) {
            logger.error("Can't write mutations to Titan store " + tableName, ex);
        }
    }
}
