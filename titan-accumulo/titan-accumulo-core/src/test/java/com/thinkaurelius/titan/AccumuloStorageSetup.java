package com.thinkaurelius.titan;

import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.AccumuloStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * Set up Accumulo storage back-end for unit tests.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloStorageSetup {

    public static AccumuloStoreManager getAccumuloStoreManager() throws StorageException {
        return getAccumuloStoreManager(getAccumuloStoreConfiguration());
    }

    public static AccumuloStoreManager getAccumuloStoreManager(Configuration config) throws StorageException {
        try {
            String backend = config.getString(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY);

            Class cls = Class.forName(backend);
            Constructor constructor = cls.getDeclaredConstructor(Configuration.class);

            return (AccumuloStoreManager) constructor.newInstance(config);
        } catch (Exception ex) {
            throw new PermanentStorageException(ex);
        }
    }

    public static Configuration getAccumuloStoreConfiguration() {
        return getAccumuloGraphConfiguration()
                .subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
    }

    public static Configuration getAccumuloGraphConfiguration() {
        BaseConfiguration config = new BaseConfiguration();

        Configuration storageConfig = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);

        storageConfig.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY,
                "com.thinkaurelius.titan.diskstorage.accumulo.TitanMockAccumuloStoreManager");
        storageConfig.addProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, "localhost");

        storageConfig.addProperty(GraphDatabaseConfiguration.AUTH_USERNAME_KEY, "root");
        storageConfig.addProperty(GraphDatabaseConfiguration.AUTH_PASSWORD_KEY, "");

        Configuration accumuloConfig = storageConfig.subset(AccumuloStoreManager.ACCUMULO_NAMESPACE);

        accumuloConfig.addProperty(AccumuloStoreManager.ACCUMULO_INSTANCE_KEY, "devdb");
        accumuloConfig.addProperty(AccumuloStoreManager.SERVER_SIDE_ITERATORS_KEY, false);

        return config;
    }

    public static void startAccumulo() throws IOException {
    }

    private static void shutdownAccumulo() throws IOException {
    }
}
