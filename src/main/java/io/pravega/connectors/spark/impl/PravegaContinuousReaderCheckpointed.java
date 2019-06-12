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
import io.pravega.client.stream.Checkpoint;
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
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class PravegaContinuousReaderCheckpointed implements ContinuousReader {

    /** The prefix of checkpoint names */
    private static final String PRAVEGA_CHECKPOINT_NAME_PREFIX = "PVG-CHK-";

    private final PravegaInputParams pravegaInputParams;

    private ReaderGroup readerGroup;

    private AtomicInteger checkpointId = new AtomicInteger(0);

    private Checkpoint checkpoint;

    public PravegaContinuousReaderCheckpointed(final PravegaInputParams pravegaInputParams) {
        this.pravegaInputParams = pravegaInputParams;
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        log.info("PravegaContinuousReader mergeOffsets called, offsets length = {}", offsets.length);
        return checkpoint != null ? new PravegaContinuousStreamOffsetCheckpointed(checkpoint) : new PravegaDummyOffsetCheckpointed();
    }

    @Override
    public Offset deserializeOffset(String json) {
        log.info("PravegaContinuousReader deserializeOffset called, {}", json);
        if (json != null && json.equals(PravegaDummyOffsetCheckpointed.OFFSET)) {
            return new PravegaDummyOffsetCheckpointed();
        } else {
            Checkpoint chkPoint = Checkpoint.fromBytes(ByteBuffer.wrap(Base64.getDecoder().decode(json)));
            return new PravegaContinuousStreamOffsetCheckpointed(chkPoint);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setStartOffset(Optional<Offset> start) {
        log.info("PravegaContinuousReader setStartOffset called, {}", start);
        if (start.isPresent()) {
            Offset offset = start.get();
            if (offset instanceof PravegaDummyOffsetCheckpointed) {
                log.info("Offset {} is instance of PravegaDummyOffsetCheckpointed", offset);
            } else if (offset instanceof PravegaContinuousStreamOffsetCheckpointed) {
                log.info("Offset {} is instance of PravegaContinuousStreamOffsetCheckpointed", offset);
                checkpoint = (Checkpoint) offset;
            } else {
                log.warn("unknown instance of OFFSET {} found", offset);
            }
        }

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig());
        readerGroupManager.createReaderGroup(pravegaInputParams.getReaderGroupName(), pravegaInputParams.getReaderGroupInfo().getReaderGroupConfig());
        this.readerGroup = readerGroupManager.getReaderGroup(pravegaInputParams.getReaderGroupName());
        if (checkpoint != null) {
            log.info("PravegaContinuousReader resetting readers to checkpoint {}", checkpoint);
            this.readerGroup.resetReadersToCheckpoint(checkpoint);
        }
        log.info("PravegaContinuousReader initialized reader group for {}, pravegaInputParams: {}", pravegaInputParams.getReaderGroupName(), pravegaInputParams);

    }

    @Override
    public Offset getStartOffset() {
        log.info("PravegaContinuousReader getStartOffset called");
        return checkpoint != null ? new PravegaContinuousStreamOffsetCheckpointed(checkpoint) : new PravegaDummyOffsetCheckpointed();
    }

    @Override
    public void commit(Offset end) {
        log.info("PravegaContinuousReader commit called, {}", end);
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        String checkpointName = PRAVEGA_CHECKPOINT_NAME_PREFIX + checkpointId.incrementAndGet();

        log.info("Initiating Pravega checkpoint {}", checkpointName);
        final CompletableFuture<Checkpoint> checkpointResult =
                this.readerGroup.initiateCheckpoint(checkpointName, scheduledExecutorService);
        try {
            checkpoint = checkpointResult.get(10000, TimeUnit.MILLISECONDS);
            log.info("Pravega checkpoint {} successfully completed", checkpoint.getName());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof TimeoutException) {
                log.warn("Pravega checkpoint {} request timedout", checkpointName);
            } else {
                log.error("Failed to create Pravega checkpoint {}", checkpointName, e);
            }
        } finally {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Override
    public void stop() {
        if (readerGroup != null) {
            readerGroup.close();
        }
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
            partitionList.add(new PravegaStreamPartitionCheckpointed(pravegaInputParams, i, checkpoint));
        }

        log.info("PravegaContinuousReader planInputPartitions called, {}", partitionList);
        return partitionList;
    }
}


@Slf4j
class PravegaStreamPartitionCheckpointed implements ContinuousInputPartition<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final int partitionId;
    private Checkpoint checkpoint;

    public PravegaStreamPartitionCheckpointed(final PravegaInputParams pravegaInputParams, final int partitionId, final Checkpoint checkpoint) {
        this.pravegaInputParams = pravegaInputParams;
        this.partitionId = partitionId;
        this.checkpoint = checkpoint;
    }

    @Override
    public InputPartitionReader<InternalRow> createContinuousReader(PartitionOffset offset) {
        log.info("PravegaStreamPartition createContinuousReader called with OFFSET {}", offset);
        return new PravegaStreamReaderCheckpointed(pravegaInputParams, partitionId, checkpoint);
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        log.info("PravegaStreamPartition createPartitionReader called");
        return new PravegaStreamReaderCheckpointed(pravegaInputParams, partitionId, checkpoint);
    }
}

@Slf4j
class PravegaStreamReaderCheckpointed implements ContinuousInputPartitionReader<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final int partitionId;
    private final ReaderGroup readerGroup;
    private final EventStreamReader<ByteBuffer> eventStreamReader;
    private EventRead<ByteBuffer> eventRead;
    private Position lastSuccessfulPosition;
    private UnsafeRow record;
    private String lastSuccessfulCheckpointName;

    @SuppressWarnings("deprecation")
    public PravegaStreamReaderCheckpointed(final PravegaInputParams pravegaInputParams, final int partitionId, final Checkpoint checkpoint) {
        this.pravegaInputParams = pravegaInputParams;
        this.partitionId = partitionId;
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig());
        readerGroupManager.createReaderGroup(pravegaInputParams.getReaderGroupName(), pravegaInputParams.getReaderGroupInfo().getReaderGroupConfig());
        this.readerGroup = readerGroupManager.getReaderGroup(pravegaInputParams.getReaderGroupName());
        if (checkpoint != null) {
            this.readerGroup.resetReadersToCheckpoint(checkpoint);
        }
        final Serializer<ByteBuffer> byteBufferDeSerializer = new ByteBufferSerializer();
        this.eventStreamReader = EventStreamClientFactory.withScope(pravegaInputParams.getReaderGroupScopeName(), pravegaInputParams.getClientConfig())
                .createReader(String.valueOf(partitionId), pravegaInputParams.getReaderGroupName(), byteBufferDeSerializer, ReaderConfig.builder().build());
        log.info("PravegaStreamReader: {}, initialized reader group {} and reader {}", partitionId, pravegaInputParams.getReaderGroupName(), eventStreamReader);
    }

    @Override
    public PartitionOffset getOffset() {
        log.info("PravegaStreamReader: {}, getOffset called. lastSuccessfulPosition = {}", partitionId, lastSuccessfulPosition);
        return lastSuccessfulPosition != null ? new PravegaStreamReaderPartitionOffsetCheckpointed(lastSuccessfulPosition.toBytes()) : null;
    }

    @Override
    public boolean next() throws IOException {
        log.info("PravegaStreamReader: {}, next called", partitionId);
        ByteBuffer event = null;
        while (event == null) {
            eventRead = eventStreamReader.readNextEvent(pravegaInputParams.getEventReadTimeoutIntervalMillis());
            event = eventRead.getEvent();
            if (eventRead.isCheckpoint()) {
                lastSuccessfulCheckpointName = eventRead.getCheckpointName();
                log.info("PravegaStreamReader: {}, checkpoint event read: {}", partitionId, lastSuccessfulCheckpointName);
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
        log.info("partition: {}, record fetched: scope={}:stream={}:segmentId={}:OFFSET={}", partitionId,
                record.getString(1), record.getString(2), record.getLong(3), record.getLong(4));

        return true;
    }

    @Override
    public InternalRow get() {
        lastSuccessfulPosition = eventRead.getPosition();
        log.info("PravegaStreamReader: {}, get called. lastSuccessfulPosition = {}", partitionId, lastSuccessfulPosition);
        return record;
    }

    @Override
    public void close() throws IOException {
        log.info("PravegaStreamReader: {}, close called. lastSuccessfulPosition = {}", partitionId, lastSuccessfulPosition);
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
class PravegaStreamReaderPartitionOffsetCheckpointed implements PartitionOffset {
    private ByteBuffer eventPointer;
    public PravegaStreamReaderPartitionOffsetCheckpointed(ByteBuffer eventPointer) {
        this.eventPointer = eventPointer;
    }
}

@Getter
@Slf4j
class PravegaContinuousStreamOffsetCheckpointed extends Offset {

    private final Checkpoint checkpoint;

    public PravegaContinuousStreamOffsetCheckpointed(final Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Override
    public String json() {
        return byteBuffer2String(checkpoint.toBytes());
    }

    static String byteBuffer2String(ByteBuffer byteBuffer) {
        return Base64.getEncoder().encodeToString(byteBuffer.array());
    }
}

class PravegaDummyOffsetCheckpointed extends Offset {
    public static final String OFFSET = "dummy";
    @Override
    public String json() {
        return OFFSET;
    }
}