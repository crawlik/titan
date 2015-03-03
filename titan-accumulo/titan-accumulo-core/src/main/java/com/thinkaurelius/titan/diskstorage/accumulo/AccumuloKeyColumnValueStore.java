package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.accumulo.util.CallableFunction;
import com.thinkaurelius.titan.diskstorage.accumulo.util.ConcurrentLists;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.titan.ColumnPaginationFilter;
import org.apache.accumulo.core.iterators.titan.ColumnSliceFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key-Column value store for Accumulo.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class AccumuloKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloKeyColumnValueStore.class);
    // Default parameters 
    private static final int NUM_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors();
    // private static final Authorizations AUTHORIZATIONS_DEFAULT = new Authorizations();
    // Instance variables
    private final AccumuloStoreManager storeManager;  // thread-safe
    private final String columnFamily;
    private final byte[] columnFamilyBytes;
    private final Text columnFamilyText;

    AccumuloKeyColumnValueStore(AccumuloStoreManager storeManager, String columnFamily) throws StorageException {
        this.storeManager = storeManager;
        this.columnFamily = columnFamily;
        this.columnFamilyBytes = columnFamily.getBytes();
        this.columnFamilyText = new Text(columnFamily);
    }

    @Override
    public String getName() {
        return columnFamily;
    }

    @Override
    public void close() throws StorageException {
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws StorageException {
        AccumuloStoreTransaction atxh = (AccumuloStoreTransaction) txh;
        String tableName = storeManager.getTableName();
        Scanner scanner;

        try {
            scanner = storeManager.getConnector().createScanner(tableName, atxh.getAuthorizations());
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        byte[] keyBytes = key.as(StaticBuffer.ARRAY_FACTORY);
        scanner.setRange(new Range(new Text(keyBytes)));
        scanner.fetchColumnFamily(columnFamilyText);

        return scanner.iterator().hasNext();
    }

    @Override
    public List<Entry> getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
        AccumuloStoreTransaction atxh = (AccumuloStoreTransaction) txh;
        String tableName = storeManager.getTableName();
        Scanner scanner;
        try {
            scanner = storeManager.getConnector().createScanner(tableName, atxh.getAuthorizations());
        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        scanner.fetchColumnFamily(columnFamilyText);

        if (query.hasLimit()) {
            IteratorSetting paginationIterator;
            paginationIterator = new IteratorSetting(10, "paginationIter", ColumnPaginationFilter.class);
            ColumnPaginationFilter.setPagination(paginationIterator, query.getLimit());

            if (!storeManager.hasServerSideIterators()) {
                scanner = new ClientSideIteratorScanner(scanner);
            }
            scanner.addScanIterator(paginationIterator);
        }

        scanner.setRange(getRange(query));

        List<Entry> slice = new ArrayList<Entry>();
        for (Map.Entry<Key, Value> kv : scanner) {
            slice.add(getEntry(kv));
        }

        return slice;
    }

    @Override
    public List<List<Entry>> getSlice(List<StaticBuffer> keys, final SliceQuery query, final StoreTransaction txh) throws StorageException {
        List<List<Entry>> slices = ConcurrentLists.transform(keys,
                new CallableFunction<StaticBuffer, List<Entry>>() {
                    @Override
                    public List<Entry> apply(StaticBuffer key) throws Exception {
                        return getSlice(new KeySliceQuery(key, query), txh);
                    }
                });

        return slices;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions,
            StoreTransaction txh) throws StorageException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        storeManager.mutateMany(ImmutableMap.of(columnFamily, mutations), txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue,
            StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StaticBuffer[] getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();  // Accumulo stores do not support local key partitions.
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws StorageException {
        AccumuloStoreTransaction atxh = (AccumuloStoreTransaction) txh;
        return executeKeySliceQuery(query.getKeyStart(), query.getKeyEnd(), query, atxh.getAuthorizations());
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
        AccumuloStoreTransaction atxh = (AccumuloStoreTransaction) txh;
        return executeKeySliceQuery(null, null, query, atxh.getAuthorizations());
    }
    
    private KeyIterator executeKeySliceQuery(StaticBuffer startKey, StaticBuffer endKey,
            SliceQuery columnSlice, Authorizations authorizations) throws StorageException {

        // Preconditions.checkArgument(!columnSlice.hasLimit() || columnSlice.getLimit() == 1);
        Scanner scanner = getKeySliceScanner(startKey, endKey, columnSlice, authorizations);

        return new RowKeyIterator(scanner);
    }

    private Scanner getKeySliceScanner(StaticBuffer startKey, StaticBuffer endKey,
            SliceQuery columnSlice, Authorizations authorizations) throws StorageException {

        String tableName = storeManager.getTableName();
        Scanner scanner;
        try {
            scanner = storeManager.getConnector().createScanner(tableName, authorizations);

        } catch (TableNotFoundException ex) {
            logger.error("Can't find Titan store " + tableName, ex);
            throw new PermanentStorageException(ex);
        }

        scanner.fetchColumnFamily(columnFamilyText);

        IteratorSetting columnSliceIterator;
        columnSliceIterator = getColumnSliceIterator(10, columnSlice);

        if (columnSliceIterator != null) {
            if (!storeManager.hasServerSideIterators()) {
                scanner = new ClientSideIteratorScanner(scanner);
            }
            scanner.addScanIterator(columnSliceIterator);
        }

        IteratorSetting columnPaginationIterator;
        columnPaginationIterator = getColumnPaginationIterator(15, columnSlice);
        
        if (columnPaginationIterator != null) {
            if (!storeManager.hasServerSideIterators() && !(scanner instanceof ClientSideIteratorScanner)) {
                scanner = new ClientSideIteratorScanner(scanner);
            }
            scanner.addScanIterator(columnPaginationIterator);
        }

        Range range = getRange(startKey, endKey);
        scanner.setRange(range);

        return scanner;
    }

    private static Entry getEntry(Map.Entry<Key, Value> keyValue) {
        byte[] colQual = keyValue.getKey().getColumnQualifier().getBytes();
        byte[] value = keyValue.getValue().get();
        return StaticBufferEntry.of(new StaticArrayBuffer(colQual), new StaticArrayBuffer(value));
    }

    private Range getRange(StaticBuffer startKey, StaticBuffer endKey) {
        Text startRow = null;
        Text endRow = null;

        if (startKey != null && startKey.length() > 0) {
            startRow = new Text(startKey.as(StaticBuffer.ARRAY_FACTORY));
        }

        if (endKey != null && endKey.length() > 0) {
            endRow = new Text(endKey.as(StaticBuffer.ARRAY_FACTORY));
        }

        return new Range(startRow, true, endRow, false);
    }

    private Range getRange(KeySliceQuery query) {
        return getRange(query.getKey(), query);
    }

    private Range getRange(StaticBuffer key) {
        return new Range(new Text(key.as(StaticBuffer.ARRAY_FACTORY)));
    }

    private Range getRange(StaticBuffer key, SliceQuery query) {
        Text row = new Text(key.as(StaticBuffer.ARRAY_FACTORY));

        Key startKey;
        Key endKey;

        if (query.getSliceStart().length() > 0) {
            startKey = new Key(row, columnFamilyText,
                    new Text(query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            startKey = new Key(row, columnFamilyText);
        }

        if (query.getSliceEnd().length() > 0) {
            endKey = new Key(row, columnFamilyText,
                    new Text(query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY)));
        } else {
            endKey = new Key(row, columnFamilyText);
        }

        return new Range(startKey, true, endKey, false);
    }
    
    private static IteratorSetting getColumnSliceIterator(int priority, SliceQuery sliceQuery) {
        IteratorSetting is = null;

        byte[] minColumn = sliceQuery.getSliceStart().as(StaticBuffer.ARRAY_FACTORY);
        byte[] maxColumn = sliceQuery.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY);

        if (minColumn.length > 0 || maxColumn.length > 0) {
            is = new IteratorSetting(priority, "columnRangeIter", ColumnSliceFilter.class);
            ColumnSliceFilter.setSlice(is, minColumn, true, maxColumn, false);
        }

        return is;
    }

    private static IteratorSetting getColumnPaginationIterator(int priority, SliceQuery sliceQuery) {
        IteratorSetting is = null;

        if (sliceQuery.hasLimit()) {
            is = new IteratorSetting(priority, "columnPaginationIter", ColumnPaginationFilter.class);
            ColumnPaginationFilter.setPagination(is, sliceQuery.getLimit());
        }

        return is;
    }

    private static class RowKeyIterator implements KeyIterator {

        RowIterator rows;
        PeekingIterator<Map.Entry<Key, Value>> currentRow;
        boolean isClosed;

        RowKeyIterator(Scanner scanner) {
            rows = new RowIterator(scanner);
            isClosed = false;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            RecordIterator<Entry> rowIter = new RecordIterator<Entry>() {
                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return currentRow.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    Map.Entry<Key, Value> kv = currentRow.next();
                    return getEntry(kv);
                }

                @Override
                public void close() {
                    isClosed = true; // same semantics as in-memory implementation in Titan core
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

            return rowIter;
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rows.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            currentRow = Iterators.peekingIterator(rows.next());
            return new StaticArrayBuffer(currentRow.peek().getKey().getRow().getBytes());
        }

        @Override
        public void close() {
            isClosed = true;
            rows = null;
            currentRow = null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }
    }
}
