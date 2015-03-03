/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.iterators.titan;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class ColumnPaginationFilterTest {

    private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

    private static final SortedMap<Key, Value> TEST_DATA = new TreeMap<Key, Value>();
    private static final Key KEY_0 = nkv(TEST_DATA, "row0", "cf", "cq0", "val0");
    private static final Key KEY_1 = nkv(TEST_DATA, "row0", "cf", "cq1", "val1");
    private static final Key KEY_2 = nkv(TEST_DATA, "row0", "cf", "cq2", "val2");
    private static final Key KEY_3 = nkv(TEST_DATA, "row1", "cf", "cq0", "val0");
    private static final Key KEY_4 = nkv(TEST_DATA, "row1", "cf", "cq1", "val1");
    private static final Key KEY_5 = nkv(TEST_DATA, "row1", "cf", "cq2", "val2");

    private static IteratorEnvironment iteratorEnvironment;

    private final ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter();
    private IteratorSetting is;

    private static Key nkv(SortedMap<Key, Value> tm, String row, String cf, String cq, String val) {
        Key k = nk(row, cf, cq);
        tm.put(k, new Value(val.getBytes()));
        return k;
    }

    private static Key nk(String row, String cf, String cq) {
        return new Key(new Text(row), new Text(cf), new Text(cq));
    }

    @Before
    public void setUp() throws Exception {
        columnPaginationFilter.describeOptions();
        iteratorEnvironment = new DefaultIteratorEnvironment();
        is = new IteratorSetting(1, ColumnSliceFilter.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeLimit() throws IOException {
        ColumnPaginationFilter.setPagination(is, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeOffset() throws IOException {
        ColumnPaginationFilter.setPagination(is, 0, -1);
    }

    @Test
    public void testEmptyPage() throws IOException {
        ColumnPaginationFilter.setPagination(is, 0, 0);

        assertTrue(columnPaginationFilter.validateOptions(is.getOptions()));
        columnPaginationFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
        columnPaginationFilter.seek(new Range(), EMPTY_COL_FAMS, true);

        assertFalse(columnPaginationFilter.hasTop());
    }

    @Test
    public void testNonexistentPage() throws IOException {
        ColumnPaginationFilter.setPagination(is, 1, TEST_DATA.size());

        assertTrue(columnPaginationFilter.validateOptions(is.getOptions()));
        columnPaginationFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
        columnPaginationFilter.seek(new Range(), EMPTY_COL_FAMS, true);

        assertFalse(columnPaginationFilter.hasTop());
    }

    @Test
    public void testFirstPage() throws IOException {
        ColumnPaginationFilter.setPagination(is, 2);

        assertTrue(columnPaginationFilter.validateOptions(is.getOptions()));
        columnPaginationFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
        columnPaginationFilter.seek(new Range(), EMPTY_COL_FAMS, true);

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_0));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_1));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_3));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_4));
        columnPaginationFilter.next();

        assertFalse(columnPaginationFilter.hasTop());
    }

    @Test
    public void testMiddlePage() throws IOException {
        ColumnPaginationFilter.setPagination(is, 2, 1);

        assertTrue(columnPaginationFilter.validateOptions(is.getOptions()));
        columnPaginationFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
        columnPaginationFilter.seek(new Range(), EMPTY_COL_FAMS, true);

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_1));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_2));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_4));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_5));
        columnPaginationFilter.next();

        assertFalse(columnPaginationFilter.hasTop());
    }

    @Test
    public void testLastPage() throws IOException {
        ColumnPaginationFilter.setPagination(is, 2, 2);

        assertTrue(columnPaginationFilter.validateOptions(is.getOptions()));
        columnPaginationFilter.init(new SortedMapIterator(TEST_DATA), is.getOptions(), iteratorEnvironment);
        columnPaginationFilter.seek(new Range(), EMPTY_COL_FAMS, true);

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_2));
        columnPaginationFilter.next();

        assertTrue(columnPaginationFilter.hasTop());
        assertTrue(columnPaginationFilter.getTopKey().equals(KEY_5));
        columnPaginationFilter.next();

        assertFalse(columnPaginationFilter.hasTop());
    }
}
