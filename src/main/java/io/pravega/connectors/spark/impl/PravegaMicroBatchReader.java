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

import com.google.common.base.Preconditions;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.connectors.spark.PravegaInputParams;
import io.pravega.connectors.spark.PravegaSchema;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Slf4j
public class PravegaMicroBatchReader implements MicroBatchReader {

    private final PravegaInputParams pravegaInputParams;
    private StreamCut startStreamCut;
    private StreamCut endStreamCut;

    public PravegaMicroBatchReader(PravegaInputParams pravegaInputParams) {
        this.pravegaInputParams = pravegaInputParams;
        this.startStreamCut = pravegaInputParams.getStartStreamCutInfo();
        this.endStreamCut = pravegaInputParams.getEndStreamCutInfo();
        log.info("MicroBatch Reader Stream: {}, Start StreamCut: {}, End StreamCut: {}", pravegaInputParams.getStream(), startStreamCut, endStreamCut);
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        if (start.isPresent()) {
            startStreamCut = StreamCut.from(start.get().toString());
        }

        if (end.isPresent()) {
            endStreamCut = StreamCut.from(end.get().toString());
        }
        //log.info("Offset Range Start StreamCut: {}, End StreamCut: {}", startStreamCut, endStreamCut);
    }

    @Override
    public Offset getStartOffset() {
        return new PravegaStreamOffset(startStreamCut);
    }

    @Override
    public Offset getEndOffset() {
        return new PravegaStreamOffset(endStreamCut);
    }

    @Override
    public Offset deserializeOffset(String json) {
        return new PravegaStreamOffset(StreamCut.from(json));
    }

    @Override
    public void commit(Offset end) {
        // do nothing
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public StructType readSchema() {
        return PravegaSchema.readSchema();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> pravegaPartitions = new ArrayList<>();
        ClientConfig clientConfig = pravegaInputParams.getClientConfig();
        String scope = pravegaInputParams.getStream().getScope();
        try ( BatchClientFactory clientFactory = BatchClientFactory.withScope(scope, clientConfig) ) {
            StreamSegmentsIterator segmentsIterator = clientFactory.getSegments(pravegaInputParams.getStream(), startStreamCut, endStreamCut);
            Iterator<SegmentRange> it = segmentsIterator.getIterator();
            while (it.hasNext()) {
                SegmentRange segmentRange = it.next();
                PravegaBatchInputPartition pravegaBatchInputPartition = new PravegaBatchInputPartition(clientConfig, segmentRange);
                pravegaPartitions.add(pravegaBatchInputPartition);
            }
        }
        log.info("Input Partitions {}", pravegaPartitions);
        return pravegaPartitions;
    }

}

@Slf4j
class PravegaBatchInputPartitionReader implements InputPartitionReader<InternalRow> {

    private final SegmentRange segmentRange;
    private final BatchClientFactory clientFactory;
    private final SegmentIterator<ByteBuffer> segmentIterator;
    private final Serializer<ByteBuffer> byteBufferDeSerializer;
    private UnsafeRow record;

    public PravegaBatchInputPartitionReader(final ClientConfig clientConfig, final SegmentRange segmentRange) {
        this.segmentRange = segmentRange;
        this.byteBufferDeSerializer = new ByteBufferSerializer();

        clientFactory = BatchClientFactory.withScope(segmentRange.getScope(), clientConfig);
        segmentIterator = clientFactory.readSegment(segmentRange, byteBufferDeSerializer);
    }

    @Override
    public boolean next() throws IOException {
        if (segmentIterator.hasNext()) {
            val event = segmentIterator.next();
            long offset = segmentIterator.getOffset();
            log.info("read event: {}, OFFSET: {}", event, offset);

            UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(5);
            unsafeRowWriter.resetRowWriter();
            unsafeRowWriter.write(0, event.array());
            unsafeRowWriter.write(1, UTF8String.fromString(segmentRange.getScope()));
            unsafeRowWriter.write(2, UTF8String.fromString(segmentRange.getStreamName()));
            unsafeRowWriter.write(3, segmentRange.getSegmentId());
            unsafeRowWriter.write(4, offset);
            record = unsafeRowWriter.getRow();
            log.info("record fetched: scope={}:stream={}:segmentId={}:OFFSET={}", record.getString(1), record.getString(2), record.getLong(3), record.getLong(4));

            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        Preconditions.checkNotNull(record);
        return record;
    }

    @Override
    public void close() throws IOException {
        log.info("closing the segment iterator: {}", segmentIterator);
        if (segmentIterator != null) {
            segmentIterator.close();
        }
        if (clientFactory != null) {
            clientFactory.close();
        }
    }
}

@Slf4j
class PravegaBatchInputPartition implements InputPartition<InternalRow> {

    private final ClientConfig clientConfig;
    private final SegmentRange segmentRange;

    public PravegaBatchInputPartition(final ClientConfig clientConfig, final SegmentRange segmentRange) {
        this.clientConfig = clientConfig;
        this.segmentRange = segmentRange;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        log.info("Creating partition reader for the config: {}, segment range: {}", clientConfig, segmentRange);
        return new PravegaBatchInputPartitionReader(clientConfig, segmentRange);
    }
}

class PravegaStreamOffset extends Offset {

    private final StreamCut streamCut;

    public PravegaStreamOffset(final StreamCut streamCut) {
        this.streamCut = streamCut;
    }

    @Override
    public String json() {
        return streamCut.asText();
    }
}
