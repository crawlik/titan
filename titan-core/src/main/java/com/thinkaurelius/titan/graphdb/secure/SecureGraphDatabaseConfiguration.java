
package com.thinkaurelius.titan.graphdb.secure;

import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import java.io.File;
import org.apache.commons.configuration.Configuration;

/**
 *
 * @author edeprit@42six.com
 */
public class SecureGraphDatabaseConfiguration extends GraphDatabaseConfiguration {

    public SecureGraphDatabaseConfiguration(String dirOrFile) {
        super(dirOrFile);
    }

    public SecureGraphDatabaseConfiguration(File dirOrFile) {
        super(dirOrFile);
    }

    public SecureGraphDatabaseConfiguration(Configuration config) {
        super(config);
    }

    @Override
    public Backend getBackend() {
        Configuration storageconfig = configuration.subset(STORAGE_NAMESPACE);
        Configuration metricsconfig = configuration.subset(METRICS_NAMESPACE);
        Backend backend = new SecureBackend(storageconfig, metricsconfig);
        backend.initialize(storageconfig);
        storeFeatures = backend.getStoreFeatures();
        return backend;
    }
}
