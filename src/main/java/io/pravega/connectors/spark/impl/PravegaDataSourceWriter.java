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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.connectors.spark.PravegaInputParams;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.pravega.connectors.spark.PravegaInputParams.EVENT_ATTRIBUTE_NAME;
import static io.pravega.connectors.spark.PravegaInputParams.ROUTING_KEY_ATTRIBUTE_NAME;

@Slf4j
public class PravegaDataSourceWriter implements DataSourceWriter {

    private final PravegaInputParams pravegaInputParams;
    private final StructType schema;

    public PravegaDataSourceWriter(PravegaInputParams pravegaInputParams, StructType schema) {
        this.pravegaInputParams = pravegaInputParams;
        this.schema = schema;
        log.info("PravegaDataSourceWriter instance created");
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new PravegaDataWriterFactory(pravegaInputParams, schema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        log.info("PravegaDataSourceWriter commit called");
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        log.info("PravegaDataSourceWriter abort called");
    }
}

class PravegaDataWriterFactory implements DataWriterFactory<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final StructType schema;

    public PravegaDataWriterFactory(PravegaInputParams pravegaInputParams, StructType schema) {
        this.pravegaInputParams = pravegaInputParams;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new PravegaDataWriter(pravegaInputParams, schema);
    }
}

@Slf4j
class PravegaDataWriter implements DataWriter<InternalRow> {

    private final EventStreamClientFactory clientFactory;
    private final EventStreamWriter<ByteBuffer> eventWriter;
    private final AtomicInteger pendingWritesCount;
    private final AtomicReference<Throwable> writeError;
    private final ExecutorService executorService;
    private final Stream stream;
    private Map<String, Integer> attributeIndexMap = new HashMap<>();

    public PravegaDataWriter(PravegaInputParams pravegaInputParams, StructType schema) {
        this.stream = pravegaInputParams.getStream();
        clientFactory = EventStreamClientFactory.withScope(pravegaInputParams.getStream().getScope(), pravegaInputParams.getClientConfig());
        Serializer<ByteBuffer> byteBufferSerializer = new ByteBufferSerializer();
        eventWriter = clientFactory.createEventWriter(pravegaInputParams.getStream().getStreamName(), byteBufferSerializer, EventWriterConfig.builder().build());
        pendingWritesCount = new AtomicInteger(0);
        writeError = new AtomicReference<>(null);
        executorService = Executors.newSingleThreadExecutor();
        traceProjection(schema);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        checkWriteError();

        String routingKey = UUID.randomUUID().toString();
        if (attributeIndexMap.containsKey(ROUTING_KEY_ATTRIBUTE_NAME)) {
            routingKey = record.getString(attributeIndexMap.get(ROUTING_KEY_ATTRIBUTE_NAME));
        }
        log.info("routingKey: {}", routingKey);

        byte[] event = record.getBinary(attributeIndexMap.get(EVENT_ATTRIBUTE_NAME));

        log.info("event: {}", event);

        this.pendingWritesCount.incrementAndGet();
        final CompletableFuture<Void> future = eventWriter.writeEvent(routingKey, ByteBuffer.wrap(event));
        future.whenCompleteAsync(
                (result, e) -> {
                    if (e != null) {
                        log.warn("Detected a write failure: {}", e);
                        writeError.compareAndSet(null, e);
                    }
                    synchronized (this) {
                        pendingWritesCount.decrementAndGet();
                        this.notify();
                    }
                },
                executorService
        );
    }

    private void checkWriteError() throws IOException {
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    void flushAndVerify() throws Exception {
        this.eventWriter.flush();

        // Wait until all errors, if any, have been recorded.
        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                this.wait();
            }
        }

        // Verify that no events have been lost so far.
        checkWriteError();
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return new PravegaWriterCommitMessage(stream, null);
    }

    @Override
    public void abort() throws IOException {
        Exception exception = null;

        try {
            flushAndVerify();
        } catch (Exception e) {
            exception = firstOrSuppressed(e, exception);
        }

        if (clientFactory != null) {
            clientFactory.close();
        }
        if (eventWriter != null) {
            eventWriter.close();
        }

        if (exception != null) {
            throw new IOException(exception);
        }
    }

    public static <T extends Throwable> T firstOrSuppressed(T newException, @Nullable T previous) {
        checkNotNull(newException, "newException");

        if (previous == null) {
            return newException;
        } else {
            previous.addSuppressed(newException);
            return previous;
        }
    }

    private void traceProjection(StructType inputSchema) {
        Iterator<StructField> it = inputSchema.iterator();
        int count = 0;
        while (it.hasNext()) {
            StructField structField = it.next();
            if (structField.name().equals(ROUTING_KEY_ATTRIBUTE_NAME) || structField.name().equals(EVENT_ATTRIBUTE_NAME)) {
                if (structField.name().equals(ROUTING_KEY_ATTRIBUTE_NAME)) {
                    if (!(structField.dataType() instanceof StringType)) {
                        throw new IllegalStateException(ROUTING_KEY_ATTRIBUTE_NAME + " is not an instance of String type. Received unsupported type " + structField.dataType().typeName());
                    }
                    attributeIndexMap.put(ROUTING_KEY_ATTRIBUTE_NAME, count);
                } else {
                    if (!(structField.dataType() instanceof BinaryType)) {
                        throw new IllegalStateException(EVENT_ATTRIBUTE_NAME + " is not an instance of byte array type. Received unsupported type " + structField.dataType().typeName());
                    }
                    attributeIndexMap.put(EVENT_ATTRIBUTE_NAME, count);
                }
            }
            count++;
        }

        if (!attributeIndexMap.containsKey(EVENT_ATTRIBUTE_NAME)) {
            throw new IllegalStateException(EVENT_ATTRIBUTE_NAME + " information from the input schema is missing");
        }

    }
}