package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStorePerformance;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import java.io.IOException;
import org.junit.BeforeClass;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AccumuloKCVSP extends KeyColumnValueStorePerformance {

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return AccumuloStorageSetup.getAccumuloStoreManager();
    }
}
