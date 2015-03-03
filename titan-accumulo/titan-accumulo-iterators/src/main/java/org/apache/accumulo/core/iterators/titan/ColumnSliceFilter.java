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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.codec.binary.Base64;

public class ColumnSliceFilter extends Filter {

    public static final String START_BOUND = "startBound";
    public static final String START_INCLUSIVE = "startInclusive";
    public static final String END_BOUND = "endBound";
    public static final String END_INCLUSIVE = "endInclusive";

    private ByteSequence startBound;
    private ByteSequence endBound;
    private boolean startInclusive;
    private boolean endInclusive;

    @Override
    public boolean accept(Key key, Value value) {
        ByteSequence colQ = key.getColumnQualifierData();
        return (startBound == null || (startInclusive ? (colQ.compareTo(startBound) >= 0) : (colQ.compareTo(startBound) > 0)))
                && (endBound == null || (endInclusive ? (colQ.compareTo(endBound) <= 0) : (colQ.compareTo(endBound) < 0)));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (options.containsKey(START_BOUND)) {
            startBound = new ArrayByteSequence(Base64.decodeBase64(options.get(START_BOUND)));
        } else {
            startBound = null;
        }

        if (options.containsKey(START_INCLUSIVE)) {
            startInclusive = Boolean.parseBoolean(options.get(START_INCLUSIVE));
        } else {
            startInclusive = true;
        }

        if (options.containsKey(END_BOUND)) {
            endBound = new ArrayByteSequence(Base64.decodeBase64(options.get(END_BOUND)));
        } else {
            endBound = null;
        }

        if (options.containsKey(END_INCLUSIVE)) {
            endInclusive = Boolean.parseBoolean(options.get(END_INCLUSIVE));
        } else {
            endInclusive = false;
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("columnSlice");
        io.setDescription("The ColumnSliceFilter/Iterator allows you to filter for key/value pairs based on a lexicographic range of column qualifier names");
        io.addNamedOption(START_BOUND, "start bound in slice");
        io.addNamedOption(END_BOUND, "end bound in slice");
        io.addNamedOption(START_INCLUSIVE, "include the start bound in the result set");
        io.addNamedOption(END_INCLUSIVE, "include the end bound in the result set");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);

        if (options.containsKey(START_BOUND)) {
            Base64.decodeBase64(options.get(START_BOUND));
        }

        if (options.containsKey(START_INCLUSIVE)) {
            Boolean.parseBoolean(options.get(START_INCLUSIVE));
        }

        if (options.containsKey(END_BOUND)) {
            Base64.decodeBase64(options.get(END_BOUND));
        }

        if (options.containsKey(END_INCLUSIVE)) {
            Boolean.parseBoolean(options.get(END_INCLUSIVE));
        }

        return true;
    }

    public static void setSlice(IteratorSetting si, String start, String end) {
        setSlice(si, start, true, end, false);
    }

    public static void setSlice(IteratorSetting si, String start, boolean startInclusive, String end, boolean endInclusive) {
        setSlice(si, (start != null) ? start.getBytes(StandardCharsets.UTF_8) : null, startInclusive,
                (end != null) ? end.getBytes(StandardCharsets.UTF_8) : null, endInclusive);
    }
    
    public static void setSlice(IteratorSetting si, byte[] start, byte[] end) {
        setSlice(si, start, true, end, false);
    }

    public static void setSlice(IteratorSetting si, byte[] start, boolean startInclusive, byte[] end, boolean endInclusive) {
        ByteSequence startBound = (start != null) ? new ArrayByteSequence(start) : null;
        ByteSequence endBound = (end != null) ? new ArrayByteSequence(end) : null;
        
        if (startBound != null && endBound != null && (startBound.compareTo(endBound) > 0 || (startBound.compareTo(endBound) == 0 && (!startInclusive || !endInclusive)))) {
            throw new IllegalArgumentException("Start key must be less than end key or equal with both sides inclusive in range (" + start + ", " + end + ")");
        }

        if (start != null) {
            si.addOption(START_BOUND, Base64.encodeBase64String(start));
        }
        if (end != null) {
            si.addOption(END_BOUND, Base64.encodeBase64String(end));
        }
        si.addOption(START_INCLUSIVE, String.valueOf(startInclusive));
        si.addOption(END_INCLUSIVE, String.valueOf(endInclusive));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        ColumnSliceFilter result = (ColumnSliceFilter) super.deepCopy(env);
        result.startBound = startBound;
        result.startInclusive = startInclusive;
        result.endBound = endBound;
        result.endInclusive = endInclusive;
        return result;
    }
}
