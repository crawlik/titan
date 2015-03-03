package org.apache.accumulo.core.iterators.titan;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

/**
 * A Filter that accepts <code>limit</code> number of key-value pairs starting at <code>offset</code>.
 *
 * @author Etienne Deprit <edeprit@42six.com>
 */
public class ColumnPaginationFilter extends Filter {

    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";

    private int limit;
    private int offset;
    private int count;
    private Text lastRow;

    @Override
    public boolean accept(Key key, Value value) {
        if (!key.getRow().equals(lastRow)) {
            lastRow = new Text(key.getRow());
            count = 0;
        }
                
        if (count >= offset + limit) {
            return false;
        }
        boolean code = count >= offset;
        count++;
        return code;
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey(LIMIT)) {
            limit = Integer.parseInt(options.get(LIMIT));
        } else {
            limit = 0;
        }

        if (options.containsKey(OFFSET)) {
            offset = Integer.parseInt(options.get(OFFSET));
        } else {
            offset = 0;
        }

        lastRow = null;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.setName("columnPagination");
        io.setDescription("The ColumnPaginationFilter/Iterator allows you to paginate results");
        io.addNamedOption(LIMIT, "maximum numger of results");
        io.addNamedOption(OFFSET, "offset of first result");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        super.validateOptions(options);

        if (options.containsKey(LIMIT)) {
            int limit;
            try {
                limit = Integer.parseInt(options.get(LIMIT));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(ex);
            }
            Preconditions.checkArgument(limit >= 0);
        }

        if (options.containsKey(OFFSET)) {
            int offset;
            try {
                offset = Integer.parseInt(options.get(OFFSET));
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(ex);
            }
            Preconditions.checkArgument(offset >= 0);
        }

        return true;
    }

    public static void setPagination(IteratorSetting is, int limit) {
        Preconditions.checkArgument(limit >= 0, "limit must be nonnegative {}", limit);
        is.addOption(ColumnPaginationFilter.LIMIT, Integer.toString(limit));
    }

    public static void setPagination(IteratorSetting is, int limit, int offset) {
        Preconditions.checkArgument(offset >= 0, "offset must be nonnegative {}", offset);
        setPagination(is, limit);
        is.addOption(ColumnPaginationFilter.OFFSET, Integer.toString(offset));
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        ColumnPaginationFilter result = (ColumnPaginationFilter) super.deepCopy(env);
        result.limit = limit;
        result.offset = offset;
        return result;
    }
}
