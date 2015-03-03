package com.thinkaurelius.titan.diskstorage.accumulo;

import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.IDAllocationTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import java.io.IOException;
import org.apache.commons.configuration.Configuration;
import org.junit.BeforeClass;

public class AccumuloIDAllocationTest extends IDAllocationTest {

    public AccumuloIDAllocationTest(Configuration baseConfig) {
        super(baseConfig);
    }

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(int idx) throws StorageException {
        return AccumuloStorageSetup.getAccumuloStoreManager();
    }
}
