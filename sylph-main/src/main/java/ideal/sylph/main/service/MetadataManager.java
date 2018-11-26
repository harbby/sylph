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
package ideal.sylph.main.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import ideal.sylph.main.server.ServerMainConfig;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.utils.GenericTypeReference;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static ideal.sylph.spi.exception.StandardErrorCode.SAVE_JOB_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * system Metadata
 */
public class MetadataManager
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final ConcurrentMap<String, String> jobInfoMetaData = new ConcurrentHashMap<>();
    private final ServerMainConfig config;
    private final File metadataFile;

    @Inject
    public MetadataManager(ServerMainConfig serverMainConfig)
    {
        this.config = requireNonNull(serverMainConfig, "serverMainConfig is null");
        this.metadataFile = new File(config.getMetadataPath(), "metadata.data");
    }

    /**
     * server start init load
     */
    public Map<String, String> loadMetadata()
            throws IOException
    {
        if (metadataFile.exists()) {
            Map<String, String> jobInfoMaps = MAPPER.readValue(metadataFile, new GenericTypeReference(Map.class, String.class, String.class));
            logger.info("loading metadata with {}", metadataFile);
            return jobInfoMaps;
        }
        return Collections.emptyMap();
    }

    public void addMetadata(String jobId, String jobInfo)
    {
        jobInfoMetaData.put(jobId, jobInfo);
        saveMetadata();
    }

    private void saveMetadata()
    {
        try {
            if (!metadataFile.exists()) {
                logger.warn("metadata file not exists {}", metadataFile);
                FileUtils.touch(metadataFile);
            }
            MAPPER.writeValue(metadataFile, jobInfoMetaData);
        }
        catch (IOException e) {
            throw new SylphException(SAVE_JOB_ERROR, "save metadata fail", e);
        }
    }

    public void removeMetadata(String jobId)
    {
        String metadata = jobInfoMetaData.remove(jobId);
        if (metadata != null) {
            saveMetadata();
        }
    }
}
