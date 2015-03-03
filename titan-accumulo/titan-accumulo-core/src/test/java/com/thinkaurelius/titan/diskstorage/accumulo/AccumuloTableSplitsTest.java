package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.Lists;
import static com.thinkaurelius.titan.AccumuloStorageSetup.getAccumuloStoreConfiguration;
import static com.thinkaurelius.titan.AccumuloStorageSetup.getAccumuloStoreManager;
import static com.thinkaurelius.titan.AccumuloStorageSetup.getAccumuloStoreManager;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import static org.junit.Assert.assertArrayEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Etienne Deprit (edeprit@42six.com)
 */
@RunWith(Parameterized.class)
public class AccumuloTableSplitsTest {

    private static final Logger log = LoggerFactory.getLogger(AccumuloTableSplitsTest.class);

    int splitBits;
    int[] splitBytes;
    AccumuloStoreManager manager;
    KeyColumnValueStore store;

    String storeName = "testStore1";

    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        List<Object[]> configurations = new ArrayList<Object[]>();

        configurations.add(new Object[]{0, new int[0]});
        configurations.add(new Object[]{1, new int[]{0x80}});
        configurations.add(new Object[]{2, new int[]{0x40, 0x80, 0xC0}});
        configurations.add(new Object[]{3, new int[]{0x20, 0x40, 0x60, 0x80, 0xA0, 0xC0, 0xE0}});
        
        return configurations;
    }

    public AccumuloTableSplitsTest(int splitBits, int[] splits) {
        this.splitBits = splitBits;
        this.splitBytes = splits;
        System.out.println("splitBits: " + splitBits);
        System.out.println("splits: " + Arrays.toString(splits));
    }

    @Before
    public void setUp() throws Exception {
        openStorageManager().clearStorage();
        open();
    }

    public void open() throws StorageException {
        manager = openStorageManager(splitBits);
        store = manager.openDatabase(storeName);
    }

    @After
    public void tearDown() throws Exception {
        close();
    }

    public void close() throws StorageException {
        store.close();
        manager.close();
    }
    
    public static AccumuloStoreManager openStorageManager() throws StorageException {
        return getAccumuloStoreManager(getAccumuloStoreConfiguration());
    }

    public static AccumuloStoreManager openStorageManager(int splitBits) throws StorageException {
        Configuration config = getAccumuloStoreConfiguration();
        
        config.subset(AccumuloStoreManager.ACCUMULO_NAMESPACE)
                .setProperty(AccumuloStoreManager.SPLIT_BITS_KEY, splitBits);
        
        return getAccumuloStoreManager(config);
    }

    @Test
    public void tableSplitsTest() throws StorageException, TableNotFoundException {
        Connector connector = manager.getConnector();
        TableOperations tableOps = connector.tableOperations();

        List<Text> splits = Lists.newArrayList();
        for (int b : splitBytes) {
            splits.add(new Text(new byte[]{(byte) b}));
        }

        assertArrayEquals(splits.toArray(),
                tableOps.getSplits(AccumuloStoreManager.TABLE_NAME_DEFAULT).toArray());
    }
}
