package com.cloudera.wdyson.flink.auditsession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class AuditInputFormat extends TextInputFormat {
    private static final Logger LOG = LoggerFactory.getLogger(AuditInputFormat.class);

    private static final float MAX_SPLIT_SIZE_DISCREPANCY = 1.1f;

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<>();
        long totalLength = 0;

        if (getFilePaths().length != 1) {
            throw new IllegalArgumentException("One audit path expected");
        }

        if (!enumerateNestedFiles) {
            throw new IllegalArgumentException("Enumerate nested files must be enabled");
        }

        Path auditPath = getFilePaths()[0];

        final FileSystem fs = auditPath.getFileSystem();
        final FileStatus auditPathFile = fs.getFileStatus(auditPath);

        HashMap<String, ArrayList<FileStatus>> pathsByDate =
            new HashMap<String, ArrayList<FileStatus>>(); 

        if (!auditPathFile.isDir()) {
            throw new IllegalArgumentException("Audit path must be a directory");
        }

        for (FileStatus serviceDir : fs.listStatus(auditPath)) {
            if (serviceDir.isDir()) {
                Path servicePath = serviceDir.getPath();

                for (FileStatus innerDir : fs.listStatus(servicePath)) {
                    if (innerDir.isDir()) {
                        Path innerPath = innerDir.getPath();

                        for (FileStatus dateDir : fs.listStatus(innerPath)) {
                            if (dateDir.isDir()) {
                                Path datePath = dateDir.getPath();
                                String date = datePath.getName();

                                if (!pathsByDate.containsKey(date)) {
                                    pathsByDate.put(date, new ArrayList<>());
                                }

                                ArrayList<FileStatus> paths = pathsByDate.get(date);

                                for (FileStatus auditDir : fs.listStatus(datePath)) {
                                    if (!auditDir.isDir()) {
                                        paths.add(auditDir);
                                    }
                                }
                            }
                        }
                    } else {
                        LOG.warn("Expected second level of children to be directories, ignoring files");
                    }
                }
            } else {
                LOG.warn("Expected first level of children to be directories, ignoring file");
            }
        }

        String[] orderedDates = pathsByDate.keySet().stream().sorted().toArray(String[]::new);

        for (String nextDate : orderedDates) {
            for (FileStatus auditDir : pathsByDate.get(nextDate)) {
                testForUnsplittable(auditDir);
                files.add(auditDir);
                totalLength += auditDir.getLen();
            }
        }

        // returns if unsplittable
        if (unsplittable) {
            LOG.debug("Files are unsplittable, will only create whole file splits.");
            int splitNum = 0;
            for (final FileStatus file : files) {
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
                Set<String> hosts = new HashSet<String>();
                for (BlockLocation block : blocks) {
                    hosts.addAll(Arrays.asList(block.getHosts()));
                }
                long len = file.getLen();
                if (testForUnsplittable(file)) {
                    len = READ_WHOLE_SPLIT_FLAG;
                }
                FileInputSplit fis =
                        new FileInputSplit(
                                splitNum++,
                                file.getPath(),
                                0,
                                len,
                                hosts.toArray(new String[hosts.size()]));
                inputSplits.add(fis);
            }
            return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
        }

        final long maxSplitSize =
                totalLength / minNumSplits + (totalLength % minNumSplits == 0 ? 0 : 1);

        // now that we have the files, generate the splits
        int splitNum = 0;
        for (final FileStatus file : files) {
            final long len = file.getLen();

            if (len > 0) {
                // get the block locations and make sure they are in order with respect to their
                // offset
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, len);
                Arrays.sort(blocks);

                if (splitAtBlocks) {
                    LOG.debug("Splitting {} at block boundaries.", file.getPath());
                    for (BlockLocation block : blocks) {
                        FileInputSplit fis =
                                new FileInputSplit(
                                        splitNum++,
                                        file.getPath(),
                                        block.getOffset(),
                                        block.getLength(),
                                        block.getHosts());
                        inputSplits.add(fis);
                    }
                } else {
                    final long blockSize = file.getBlockSize();
                    final long minSplitSize;
                    if (this.minSplitSize <= blockSize) {
                        minSplitSize = this.minSplitSize;
                    } else {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn(
                                    "Minimal split size of "
                                            + this.minSplitSize
                                            + " is larger than the block size of "
                                            + blockSize
                                            + ". Decreasing minimal split size to block size.");
                        }
                        minSplitSize = blockSize;
                    }

                    final long splitSize =
                            Math.max(minSplitSize, Math.min(maxSplitSize, blockSize));
                    final long halfSplit = splitSize >>> 1;

                    final long maxBytesForLastSplit =
                            (long) (splitSize * MAX_SPLIT_SIZE_DISCREPANCY);

                    long bytesUnassigned = len;
                    long position = 0;

                    int blockIndex = 0;

                    while (bytesUnassigned > maxBytesForLastSplit) {
                        // get the block containing the majority of the data
                        blockIndex = getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                        // create a new split
                        FileInputSplit fis =
                                new FileInputSplit(
                                        splitNum++,
                                        file.getPath(),
                                        position,
                                        splitSize,
                                        blocks[blockIndex].getHosts());
                        inputSplits.add(fis);

                        // adjust the positions
                        position += splitSize;
                        bytesUnassigned -= splitSize;
                    }

                    // assign the last split
                    if (bytesUnassigned > 0) {
                        blockIndex =
                                getBlockIndexForPosition(blocks, position, halfSplit, blockIndex);
                        final FileInputSplit fis =
                                new FileInputSplit(
                                        splitNum++,
                                        file.getPath(),
                                        position,
                                        bytesUnassigned,
                                        blocks[blockIndex].getHosts());
                        inputSplits.add(fis);
                    }
                }
            } else {
                // special case with a file of zero bytes size
                final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 0);
                String[] hosts;
                if (blocks.length > 0) {
                    hosts = blocks[0].getHosts();
                } else {
                    hosts = new String[0];
                }
                final FileInputSplit fis =
                        new FileInputSplit(splitNum++, file.getPath(), 0, 0, hosts);
                inputSplits.add(fis);
            }
        }

        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);
    }

    private int getBlockIndexForPosition(
            BlockLocation[] blocks, long offset, long halfSplitSize, int startIndex) {
        // go over all indexes after the startIndex
        for (int i = startIndex; i < blocks.length; i++) {
            long blockStart = blocks[i].getOffset();
            long blockEnd = blockStart + blocks[i].getLength();

            if (offset >= blockStart && offset < blockEnd) {
                // got the block where the split starts
                // check if the next block contains more than this one does
                if (i < blocks.length - 1 && blockEnd - offset < halfSplitSize) {
                    return i + 1;
                } else {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("The given offset is not contained in the any block.");
    }

    public AuditInputFormat(Path filePath) {
        super(filePath);

        enumerateNestedFiles = true;
    }
}
