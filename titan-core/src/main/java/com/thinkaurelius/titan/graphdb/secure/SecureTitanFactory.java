package com.thinkaurelius.titan.graphdb.secure;

import com.thinkaurelius.titan.core.TitanGraph;
import java.io.File;
import org.apache.commons.configuration.Configuration;

/**
 *
 * @author edeprit@42six.com
 */
public class SecureTitanFactory {
    
    /**
     * Opens a {@link TitanGraph} database.
     * <p/>
     * If the argument points to a configuration file, the configuration file is loaded to configure the database.
     * If the argument points to a path, a graph database is created or opened at that location.
     *
     * @param directoryOrConfigFile Configuration file name or directory name
     * @return Secure Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static SecureTitanGraph open(String directoryOrConfigFile) {
        return open(SecureGraphDatabaseConfiguration.getConfiguration(new File(directoryOrConfigFile)));
    }

    /**
     * Opens a {@link TitanGraph} database configured according to the provided configuration.
     *
     * @param configuration Configuration for the graph database
     * @return Secure Titan graph database
     * @see <a href="https://github.com/thinkaurelius/titan/wiki/Graph-Configuration">Graph Configuration Wiki</a>
     */
    public static SecureTitanGraph open(Configuration configuration) {
        return new SecureTitanGraph(new SecureGraphDatabaseConfiguration(configuration));
    }
}