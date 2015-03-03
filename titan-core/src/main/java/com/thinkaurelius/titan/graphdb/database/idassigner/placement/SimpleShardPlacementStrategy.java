package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;
import static com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS_DEFAULT;
import static com.thinkaurelius.titan.graphdb.database.idassigner.placement.SimpleBulkPlacementStrategy.CONCURRENT_PARTITIONS_KEY;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @author edeprit (edeprit@42six.com)
 */
public class SimpleShardPlacementStrategy implements IDPlacementStrategy {

    private static final Logger log = LoggerFactory.getLogger(SimpleShardPlacementStrategy.class);

    public static final String IDS_SHARD_BITS_KEY = "shard-bits";
    public static final int IDS_SHARD_BITS_DEFAULT = 4;

    private static final int EXHAUSTED_SHARD = -1;

    private final Random random = new Random();
    private final int[] shardPartitions;
    private final int shardBits;
    private final int maxShardID;
    private volatile int partitionBits;
    private volatile int suffixBits;
    private volatile int maxSuffixID;

    public SimpleShardPlacementStrategy(int shardBits) {
        Preconditions.checkArgument(shardBits > 0);
        this.shardBits = shardBits;
        maxShardID = 1 << shardBits;
        shardPartitions = new int[maxShardID];
    }

    public SimpleShardPlacementStrategy(Configuration config) {
        this(config.getInt(IDS_SHARD_BITS_KEY, IDS_SHARD_BITS_DEFAULT));
    }

    @Override
    public int getPartition(InternalElement vertex) {
        return nextPartitionID();
    }

    @Override
    public boolean supportsBulkPlacement() {
        return true;
    }

    @Override
    public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
        int partitionID = nextPartitionID();
        for (Map.Entry<InternalVertex, PartitionAssignment> entry : vertices.entrySet()) {
            entry.setValue(new SimplePartitionAssignment(partitionID));
        }
    }

    @Override
    public void setLocalPartitionBounds(int lowerID, int upperID, int idLimit) {
        Preconditions.checkArgument(idLimit > 0);
        Preconditions.checkArgument(lowerID == 0);
        Preconditions.checkArgument(upperID == idLimit);

        partitionBits = Integer.SIZE - Integer.numberOfLeadingZeros(upperID) - 1;
        Preconditions.checkState(shardBits < partitionBits);

        suffixBits = partitionBits - shardBits;
        maxSuffixID = 1 << suffixBits;
    }

    @Override
    public void exhaustedPartition(int partitionID) {
        int shard = shardID(partitionID);
        if (shardPartitions[shard] < maxSuffixID) {
            shardPartitions[shard]++;
        }
    }

    private int nextPartitionID() {
        int suffix = maxSuffixID;
        int shard = 0;
        while (suffix == maxSuffixID) {
            shard = random.nextInt(shardPartitions.length);
            suffix = shardPartitions[shard];
        }
        return partitionID(shard, suffix);
    }

    private int shardID(int partitionID) {
        return partitionID >> suffixBits;
    }

    private int partitionID(int shard, int suffix) {
        Preconditions.checkArgument(shard >= 0);
        Preconditions.checkArgument(suffix >= 0);
        return (shard << suffixBits) | suffix;
    }
}
