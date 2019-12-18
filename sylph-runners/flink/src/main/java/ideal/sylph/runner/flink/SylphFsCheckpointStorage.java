/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.runner.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import javax.annotation.Nullable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * copy {@link org.apache.flink.runtime.state.filesystem.FsCheckpointStorage}
 * An implementation of durable checkpoint storage to file systems.
 */
public class SylphFsCheckpointStorage
        extends AbstractFsCheckpointStorage
{
    private final FileSystem fileSystem;

    private final Path checkpointsDirectory;

    private final Path sharedStateDirectory;

    private final Path taskOwnedStateDirectory;

    private final int fileSizeThreshold;

    private final int writeBufferSize;

    public SylphFsCheckpointStorage(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold)
            throws IOException
    {
        this(checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                jobId,
                fileSizeThreshold,
                CheckpointingOptions.FS_WRITE_BUFFER_SIZE.defaultValue()); //todo:FsStateBackend#getWriteBufferSize
    }

    public SylphFsCheckpointStorage(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException
    {
        super(jobId, defaultSavepointDirectory);

        checkArgument(fileSizeThreshold >= 0);

        this.fileSystem = requireNonNull(fs);
        //this.checkpointsDirectory = getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId);
        this.checkpointsDirectory = checkpointBaseDirectory;
        this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        this.taskOwnedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        this.fileSizeThreshold = fileSizeThreshold;
        this.writeBufferSize = writeBufferSize;

        // initialize the dedicated directories
        fileSystem.mkdirs(checkpointsDirectory);
        fileSystem.mkdirs(sharedStateDirectory);
        fileSystem.mkdirs(taskOwnedStateDirectory);
    }

    // ------------------------------------------------------------------------

    public Path getCheckpointsDirectory()
    {
        return checkpointsDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage()
    {
        return true;
    }

    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException
    {
        checkArgument(checkpointId >= 0);

        // prepare all the paths needed for the checkpoints
        final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

        // create the checkpoint exclusive directory
        fileSystem.mkdirs(checkpointDir);

        return new FsCheckpointStorageLocation(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                CheckpointStorageLocationReference.getDefault(),
                fileSizeThreshold,
                writeBufferSize);
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId,
            CheckpointStorageLocationReference reference)
            throws IOException
    {
        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsCheckpointStorageLocation(
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        }
        else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsCheckpointStorageLocation(
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        }
    }

    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream()
            throws IOException
    {
        return new FsCheckpointStateOutputStream(
                taskOwnedStateDirectory,
                fileSystem,
                CheckpointingOptions.FS_WRITE_BUFFER_SIZE.defaultValue(), //todo:FsStateBackend#getWriteBufferSize
                fileSizeThreshold);
    }

    @Override
    protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location)
            throws IOException
    {
        final CheckpointStorageLocationReference reference = encodePathAsReference(location);
        return new FsCheckpointStorageLocation(fs, location, location, location, reference, fileSizeThreshold, writeBufferSize);
    }
}
