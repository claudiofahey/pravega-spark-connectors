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
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.common.Exceptions;
import io.pravega.connectors.spark.PravegaInputParams;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.pravega.connectors.spark.PravegaInputParams.EVENT_ATTRIBUTE_NAME;
import static io.pravega.connectors.spark.PravegaInputParams.ROUTING_KEY_ATTRIBUTE_NAME;

@Slf4j
public class PravegaStreamWriter implements StreamWriter {

    private final PravegaInputParams pravegaInputParams;
    private final StructType schema;

    public PravegaStreamWriter(PravegaInputParams pravegaInputParams, StructType schema) {
        this.pravegaInputParams = pravegaInputParams;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new PravegaWriterFactory(pravegaInputParams, schema);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        log.info("PravegaStreamWriter commit called. epoch: {}", epochId);
        if (epochId >= 0) {
            for (WriterCommitMessage writerCommitMessage: messages) {
                PravegaWriterCommitMessage pravegaWriterCommitMessage = (PravegaWriterCommitMessage) writerCommitMessage;
                Stream stream = pravegaWriterCommitMessage.getStream();
                UUID txnId = pravegaWriterCommitMessage.getTxnId();
                Serializer<ByteBuffer> byteBufferSerializer = new ByteBufferSerializer();
                try (
                        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(stream.getScope(), pravegaInputParams.getClientConfig());
                        TransactionalEventStreamWriter<ByteBuffer> eventWriter = clientFactory.createTransactionalEventWriter(stream.getStreamName(), byteBufferSerializer, EventWriterConfig.builder().build());
                ) {
                    Transaction currentTxn = eventWriter.getTxn(txnId);
                    if (currentTxn.checkStatus() == Transaction.Status.COMMITTING || currentTxn.checkStatus() == Transaction.Status.COMMITTED ) {
                        log.info("transaction {} is already committed or in the process of committing", txnId);
                        continue;
                    }

                    if (currentTxn.checkStatus() == Transaction.Status.ABORTED || currentTxn.checkStatus() == Transaction.Status.ABORTING ) {
                        log.warn("transaction {} is already aborting or in the process of aborting", txnId);
                        continue;
                    }

                    currentTxn.commit();
                    log.info("successfully committed the transaction {}", txnId);
                } catch (TxnFailedException e) {
                    log.error("failed to commit the transaction {}", txnId, e);
                }
            }
        }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        commit(-1, messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        log.info("PravegaStreamWriter abort called. epoch: {}", epochId);
        if (epochId >= 0) {
            for (WriterCommitMessage writerCommitMessage: messages) {
                PravegaWriterCommitMessage pravegaWriterCommitMessage = (PravegaWriterCommitMessage) writerCommitMessage;
                Stream stream = pravegaWriterCommitMessage.getStream();
                UUID txnId = pravegaWriterCommitMessage.getTxnId();
                Serializer<ByteBuffer> byteBufferSerializer = new ByteBufferSerializer();
                try (
                        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(stream.getScope(), pravegaInputParams.getClientConfig());
                        TransactionalEventStreamWriter<ByteBuffer> eventWriter = clientFactory.createTransactionalEventWriter(stream.getStreamName(), byteBufferSerializer, EventWriterConfig.builder().build());
                ) {
                    Transaction currentTxn = eventWriter.getTxn(txnId);
                    if (currentTxn.checkStatus() == Transaction.Status.OPEN) {
                        currentTxn.abort();
                        log.info("successfully aborted the transaction {}", txnId);
                    }
                } catch (Exception e) {
                    log.error("failed to abort the transaction {}", txnId, e);
                }
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        abort(-1, messages);
    }
}

class PravegaWriterFactory implements DataWriterFactory<InternalRow> {

    private final PravegaInputParams pravegaInputParams;
    private final StructType schema;

    public PravegaWriterFactory(PravegaInputParams pravegaInputParams, StructType schema) {
        this.pravegaInputParams = pravegaInputParams;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
        return new PravegaWriter(pravegaInputParams, schema);
    }
}

@Slf4j
class PravegaWriter implements DataWriter<InternalRow> {

    private final EventStreamClientFactory clientFactory;
    private final TransactionalEventStreamWriter<ByteBuffer> eventWriter;
    private final Stream stream;
    private Map<String, Integer> attributeIndexMap = new HashMap<>();
    private Transaction<ByteBuffer> currentTxn;

    public PravegaWriter(PravegaInputParams pravegaInputParams, StructType schema) {
        this.stream = pravegaInputParams.getStream();
        clientFactory = EventStreamClientFactory.withScope(pravegaInputParams.getStream().getScope(), pravegaInputParams.getClientConfig());
        Serializer<ByteBuffer> byteBufferSerializer = new ByteBufferSerializer();
        eventWriter = clientFactory.createTransactionalEventWriter(pravegaInputParams.getStream().getStreamName(), byteBufferSerializer, EventWriterConfig.builder().build());
        currentTxn = eventWriter.beginTxn();
        log.info("Opened new transaction in constructor {}", currentTxn.getTxnId());
        traceProjection(schema);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        String routingKey = UUID.randomUUID().toString();
        if (attributeIndexMap.containsKey(ROUTING_KEY_ATTRIBUTE_NAME)) {
            routingKey = record.getString(attributeIndexMap.get(ROUTING_KEY_ATTRIBUTE_NAME));
        }
        log.info("routingKey: {}", routingKey);

        byte[] event = record.getBinary(attributeIndexMap.get(EVENT_ATTRIBUTE_NAME));
        log.info("event: {}", event);

        try {
            currentTxn.writeEvent(routingKey, ByteBuffer.wrap(event));
        } catch (TxnFailedException e) {
            log.error("failed to write the event", e);
            throw new IOException(e);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        log.info("flushing the messages for the transaction {}", currentTxn);
        try {
            currentTxn.flush();
        } catch (TxnFailedException e) {
            log.error("failed to flush the transaction", e);
            throw new IOException(e);
        }
        UUID txnId = currentTxn.getTxnId();
        currentTxn = eventWriter.beginTxn();
        log.info("Opened new transaction after flush {}", currentTxn.getTxnId());
        return new PravegaWriterCommitMessage(stream, txnId);
    }

    @Override
    public void abort() throws IOException {
        Exception exception = null;

        Transaction<?> txn = this.currentTxn;
        if (txn != null) {
            try {
                Exceptions.handleInterrupted(txn::abort);
            } catch (Exception e) {
                exception = e;
            }
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

@Getter
class PravegaWriterCommitMessage implements WriterCommitMessage {
    private final Stream stream;
    private final UUID txnId;
    public PravegaWriterCommitMessage(Stream stream, UUID txnId) {
        this.stream = stream;
        this.txnId = txnId;
    }
}
