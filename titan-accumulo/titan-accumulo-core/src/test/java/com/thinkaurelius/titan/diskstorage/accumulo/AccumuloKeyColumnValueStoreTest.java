package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.AccumuloStorageSetup;
import com.thinkaurelius.titan.diskstorage.KeyColumnValueStoreTest;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class AccumuloKeyColumnValueStoreTest extends KeyColumnValueStoreTest {
    
    public AccumuloStoreManager accumuloManager;

    @BeforeClass
    public static void startAccumulo() throws IOException {
        AccumuloStorageSetup.startAccumulo();
    }
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        accumuloManager = (AccumuloStoreManager) manager;
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws StorageException {
        return AccumuloStorageSetup.getAccumuloStoreManager();
    }

    public Map<String, Set<Text>> localityGroups(String... dbs) {
        Map<String, Set<Text>> localityGroups = Maps.newHashMap();
        for (String db : dbs) {
            localityGroups.put(db, Sets.newHashSet(new Text(db)));
        }
        return localityGroups;
    }

    @Test
    public void localityGroupOneStore() throws TableNotFoundException, AccumuloException {
        Connector connector = accumuloManager.getConnector();
        TableOperations tableOps = connector.tableOperations();
        
        Map<String, Set<Text>> localityGroups = localityGroups(storeName);
        assertEquals(localityGroups(storeName), tableOps.getLocalityGroups(AccumuloStoreManager.TABLE_NAME_DEFAULT));
    }

    @Test
    public void localityGroupAddStore() throws TableNotFoundException, AccumuloException, StorageException {
        Connector connector = accumuloManager.getConnector();
        TableOperations tableOps = connector.tableOperations();
        
        String storeName2 = "testStore2";
        manager.openDatabase(storeName2);
        
        Map<String, Set<Text>> localityGroups = localityGroups(storeName, storeName2);
        assertEquals(localityGroups, tableOps.getLocalityGroups(AccumuloStoreManager.TABLE_NAME_DEFAULT));
    }
}
