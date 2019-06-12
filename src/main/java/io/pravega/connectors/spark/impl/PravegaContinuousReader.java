/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.spark.impl;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.connectors.spark.PravegaInputParams;
import io.pravega.connectors.spark.PravegaSchema;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.sources.v2.reader.ContinuousInputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Slf4j
public class PravegaContinuousReader implements ContinuousReader {

    private final PravegaInputParams pravegaInputParams;

    public PravegaContinuousReader(final PravegaInputParams pravegaInputParams) {
        this.pravegaInputParams = pravegaInputParams;
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig());
        readerGroupManager.createReaderGroup(pravegaInputParams.getReaderGroupName(), pravegaInputParams.getReaderGroupInfo().getReaderGroupConfig());
        log.info("PravegaContinuousReader initialized reader group for {}, pravegaInputParams: {}", pravegaInputParams.getReaderGroupName(), pravegaInputParams);
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        log.info("PravegaContinuousReader mergeOffsets called, offsets length = {}", offsets.length);
        /*
        if (offsets.length > 0 && offsets[0] != null) {
            PravegaStreamReaderPartitionOffset pravegaStreamReaderPartitionOffset = (PravegaStreamReaderPartitionOffset) offsets[0];
            PravegaContinuousStreamOffset OFFSET = new PravegaContinuousStreamOffset(pravegaStreamReaderPartitionOffset);
            log.info("returing OFFSET: {}", OFFSET.json());
            return OFFSET;
        }
        */
        return new PravegaDummyOffset();
    }

    @Override
    public Offset deserializeOffset(String json) {
        log.info("PravegaContinuousReader deserializeOffset called, {}", json);
        return new PravegaDummyOffset();
    }

    @Override
    public void setStartOffset(Optional<Offset> start) {
        log.info("PravegaContinuousReader setStartOffset called, {}", start);
    }

    @Override
    public Offset getStartOffset() {
        log.info("PravegaContinuousReader getStartOffset called");
        return null;
    }

    @Override
    public void commit(Offset end) {
        log.info("PravegaContinuousReader commit called, {}", end);
    }

    @Override
    public void stop() {
        log.info("PravegaContinuousReader stop called");
    }

    @Override
    public StructType readSchema() {
        return PravegaSchema.readSchema();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {

        List<InputPartition<InternalRow>> partitionList = new ArrayList<>();
        for (int i = 1; i <= pravegaInputParams.getStreamReaderParallelism(); i++) {
            partitionList.add(new PravegaStreamPartition(pravegaInputParams, i));
        }

        log.info("PravegaContinuousReader planInputPartitions called, {}", partitionList);
        return partitionList;
    }
}


@Slf4j
class PravegaStreamPartition implements ContinuousInputPartition<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final int partitionId;

    public PravegaStreamPartition(final PravegaInputParams pravegaInputParams, final int partitionId) {
        this.pravegaInputParams = pravegaInputParams;
        this.partitionId = partitionId;
    }

    @Override
    public InputPartitionReader<InternalRow> createContinuousReader(PartitionOffset offset) {
        log.info("PravegaStreamPartition createContinuousReader called with OFFSET {}", offset);
        return new PravegaStreamReader(pravegaInputParams, partitionId);
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        log.info("PravegaStreamPartition createPartitionReader called");
        return new PravegaStreamReader(pravegaInputParams, partitionId);
    }
}

@Slf4j
class PravegaStreamReader implements ContinuousInputPartitionReader<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final int partitionId;
    private final ReaderGroup readerGroup;
    private final EventStreamReader<ByteBuffer> eventStreamReader;
    private EventRead<ByteBuffer> eventRead;
    private Position lastSuccessfulPosition;
    private UnsafeRow record;
    private String lastSuccessfulCheckpointName;

    public PravegaStreamReader(final PravegaInputParams pravegaInputParams, final int partitionId) {
        this.pravegaInputParams = pravegaInputParams;
        this.partitionId = partitionId;
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig());
        readerGroupManager.createReaderGroup(pravegaInputParams.getReaderGroupName(), pravegaInputParams.getReaderGroupInfo().getReaderGroupConfig());
        this.readerGroup = readerGroupManager.getReaderGroup(pravegaInputParams.getReaderGroupName());
        final Serializer<ByteBuffer> byteBufferDeSerializer = new ByteBufferSerializer();
        this.eventStreamReader = EventStreamClientFactory.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig())
                .createReader(String.valueOf(partitionId), pravegaInputParams.getReaderGroupName(), byteBufferDeSerializer, ReaderConfig.builder().build());
        log.info("PravegaStreamReader initialized reader group {} and reader {}", pravegaInputParams.getReaderGroupName(), eventStreamReader);
    }

    @Override
    public PartitionOffset getOffset() {
        log.info("PravegaStreamReader getOffset called. lastSuccessfulPosition = {}", lastSuccessfulPosition);
        return lastSuccessfulPosition != null ? new PravegaStreamReaderPartitionOffset(lastSuccessfulPosition.toBytes()) : null;
    }

    @Override
    public boolean next() throws IOException {
        log.info("PravegaStreamReader next called");
        ByteBuffer event = null;
        while (event == null) {
            eventRead = eventStreamReader.readNextEvent(pravegaInputParams.getEventReadTimeoutIntervalMillis());
            event = eventRead.getEvent();
            if (eventRead.isCheckpoint()) {
                lastSuccessfulCheckpointName = eventRead.getCheckpointName();
                log.info("PravegaStreamReader checkpoint event read: {}", lastSuccessfulCheckpointName);
            }
        }
        UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(5);
        unsafeRowWriter.resetRowWriter();
        unsafeRowWriter.write(0, event.array());
        unsafeRowWriter.write(1, UTF8String.fromString(pravegaInputParams.getStream().getScope()));
        unsafeRowWriter.write(2, UTF8String.fromString(pravegaInputParams.getStream().getStreamName()));
        unsafeRowWriter.write(3, -1);
        unsafeRowWriter.write(4, -1);
        record = unsafeRowWriter.getRow();
        log.info("record fetched: scope={}:stream={}:segmentId={}:OFFSET={}", record.getString(1), record.getString(2), record.getLong(3), record.getLong(4));

        return true;
    }

    @Override
    public InternalRow get() {
        lastSuccessfulPosition = eventRead.getPosition();
        log.info("PravegaStreamReader get called. lastSuccessfulPosition = {}", lastSuccessfulPosition);
        return record;
    }

    @Override
    public void close() throws IOException {
        log.info("PravegaStreamReader close called. lastSuccessfulPosition = {}", lastSuccessfulPosition);
        if (readerGroup != null) {
            if (lastSuccessfulPosition != null) {
                readerGroup.readerOffline(String.valueOf(partitionId), lastSuccessfulPosition);
            }
            readerGroup.close();
        }

        if (eventStreamReader != null) {
            eventStreamReader.close();
        }
    }
}

@Getter
class PravegaStreamReaderPartitionOffset implements PartitionOffset {

    private ByteBuffer eventPointer;
    public PravegaStreamReaderPartitionOffset(ByteBuffer eventPointer) {
        this.eventPointer = eventPointer;
    }

}

@Getter
@Slf4j
class PravegaContinuousStreamOffset extends Offset {

    private final PravegaStreamReaderPartitionOffset pravegaStreamReaderPartitionOffset;

    public PravegaContinuousStreamOffset(final PravegaStreamReaderPartitionOffset pravegaStreamReaderPartitionOffset) {
        this.pravegaStreamReaderPartitionOffset = pravegaStreamReaderPartitionOffset;
    }

    @Override
    public String json() {
        return Position.fromBytes(pravegaStreamReaderPartitionOffset.getEventPointer()).toString();
    }
}

class PravegaDummyOffset extends Offset {
    public static final String OFFSET = "dummy";
    @Override
    public String json() {
        return OFFSET;
    }
}