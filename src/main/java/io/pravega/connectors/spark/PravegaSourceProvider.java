/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.spark;

import io.pravega.connectors.spark.impl.PravegaContinuousReaderCheckpointed;
import io.pravega.connectors.spark.impl.PravegaDataSourceWriter;
import io.pravega.connectors.spark.impl.PravegaMicroBatchReader;
import io.pravega.connectors.spark.impl.PravegaStreamWriter;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class PravegaSourceProvider implements DataSourceV2, DataSourceRegister,
        ContinuousReadSupport, MicroBatchReadSupport, ReadSupport, StreamWriteSupport, WriteSupport {

    public static final String ALIAS = "pravega";

    @Override
    public String shortName() {
        return ALIAS;
    }

    @Override
    public ContinuousReader createContinuousReader(Optional<StructType> schema,
                                                   String checkpointLocation,
                                                   DataSourceOptions options) {
        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options.asMap(), PravegaInputParams.getMandatoryArgs());
        return new PravegaContinuousReaderCheckpointed(pravegaInputParams);
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> schema,
                                                   String checkpointLocation,
                                                   DataSourceOptions options) {
        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options.asMap(), PravegaInputParams.getMandatoryArgs());
        return new PravegaMicroBatchReader(pravegaInputParams);
    }

    @Override
    public StreamWriter createStreamWriter(String queryId,
                                           StructType schema,
                                           OutputMode mode,
                                           DataSourceOptions options) {
        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options.asMap(), PravegaInputParams.getMandatoryArgs());
        return new PravegaStreamWriter(pravegaInputParams, schema);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode, DataSourceOptions options) {
        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options.asMap(), PravegaInputParams.getMandatoryArgs());
        PravegaDataSourceWriter writer = new PravegaDataSourceWriter(pravegaInputParams, schema);
        return Optional.of(writer);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options.asMap(), PravegaInputParams.getMandatoryArgs());
        return new PravegaMicroBatchReader(pravegaInputParams);
    }
}
