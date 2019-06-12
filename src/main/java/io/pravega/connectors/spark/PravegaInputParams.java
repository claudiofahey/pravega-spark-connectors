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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@AllArgsConstructor
@Slf4j
@ToString
public final class PravegaInputParams implements Serializable {

    public static final String CONTROLLER_URI_KEY = "controller";

    public static final String SCOPE_NAME_KEY = "scope";

    public static final String STREAM_NAME_KEY = "stream";

    public static final String START_STREAMCUT_NAME_KEY = "startstreamcut";

    public static final String END_STREAMCUT_NAME_KEY = "endstreamcut";

    public static final String SERIALIZER_CLASS_NAME_KEY = "serializer";

    public static final String DESERIALIZER_CLASS_NAME_KEY = "deserializer";

    public static final String TRUSTSTORE_FILE_PATH_KEY = "truststore";

    public static final String ROUTING_KEY_ATTRIBUTE_NAME = "routingkey";

    public static final String EVENT_ATTRIBUTE_NAME = "event";


    public static final String READER_GROUP_NAME_KEY = "readergroupname";

    public static final String READER_GROUP_SCOPE_NAME_KEY = "readergroupscopename";

    public static final String MAX_OUTSTANDING_CHECKPOINT_REQUEST_KEY = "maxoutstandingcheckpointrequest";

    public static final String READER_GROUP_REFRESH_INTERVAL_MILLIS_KEY = "readergrouprefreshintervalmillis";

    public static final String EVENT_READ_TIMEOUT_INTERVAL_MILLIS_KEY = "eventreadtimeoutintervalmillis";

    public static final String AUTOMATIC_CHECKPOINT_INTERVAL_MILLIS_KEY = "automaticcheckpointintervalmillis";

    public static final String STREAM_READER_PARALLELISM_KEY = "streamreaderparallelism";

    public static final String READER_GROUP_NAME_DEFAULT = UUID.randomUUID().toString();

    public static final int MAX_OUTSTANDING_CHECKPOINT_REQUEST_DEFAULT = 3;

    public static final long READER_GROUP_REFRESH_INTERVAL_MILLIS_DEFAULT = 3000;

    public static final long EVENT_READ_TIMEOUT_INTERVAL_MILLIS_DEFAULT = 1000;

    public static final long AUTOMATIC_CHECKPOINT_INTERVAL_MILLIS_DEFAULT = 3000;

    public static final int STREAM_READER_PARALLELISM_DEFAULT = 1;

    @Getter
    private final Stream stream;

    @Getter
    private final URI controllerUri;

    @Getter
    private final String startStreamcut;

    @Getter
    private final String endStreamcut;

    @Getter
    private final String serializer;

    @Getter
    private final String deserializer;

    @Getter
    private final String truststore;

    @Getter
    private final String readerGroupName;

    @Getter
    private final String readerGroupScopeName;

    @Getter
    private final int maxOutstandingCheckpointRequest;

    @Getter
    private final long readerGroupRefreshIntervalMillis;

    @Getter
    private final long eventReadTimeoutIntervalMillis;

    @Getter
    private final long automaticCheckpointIntervalMillis;

    @Getter
    private final int streamReaderParallelism;

    public ClientConfig getClientConfig() {
        ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder()
                .controllerURI(controllerUri)
                .trustStore(truststore);
        return clientConfigBuilder.build();
    }

    public Serializer<?> getSerializerInstance() {
        try {
            Class<?> deserializerClass = Class.forName(serializer);
            return (Serializer<?>) deserializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating serializer for {}", serializer, e);
            throw new IllegalArgumentException("Unable to create the event serializer (" + serializer + ")", e);
        }
    }

    public Serializer<?> getDeSerializerInstance() {
        try {
            Class<?> deserializerClass = Class.forName(deserializer);
            return (Serializer<?>) deserializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating deserializer for {}", deserializer, e);
            throw new IllegalArgumentException("Unable to create the event deserializer (" + deserializer + ")", e);
        }
    }

    public StreamCut getStartStreamCutInfo() {
        StreamCut streamCut = StreamCut.UNBOUNDED;
        if (startStreamcut != null && startStreamcut.length() != 0) {
            streamCut = StreamCut.from(startStreamcut);
        }
        return streamCut;
    }

    public StreamCut getEndStreamCutInfo() {
        StreamCut streamCut = StreamCut.UNBOUNDED;
        if (endStreamcut != null && endStreamcut.length() != 0) {
            streamCut = StreamCut.from(endStreamcut);
        }
        return streamCut;
    }

    public ReaderGroupInfo getReaderGroupInfo() {
        ReaderGroupConfig.ReaderGroupConfigBuilder rgConfigBuilder = ReaderGroupConfig
                .builder()
                .maxOutstandingCheckpointRequest(this.maxOutstandingCheckpointRequest)
                .disableAutomaticCheckpoints();

        rgConfigBuilder.groupRefreshTimeMillis(readerGroupRefreshIntervalMillis);
        rgConfigBuilder.stream(stream);

        return new ReaderGroupInfo(rgConfigBuilder.build(), readerGroupScopeName, readerGroupName);
    }

    public static PravegaInputParams build(Map<String, String> options, Set<String> mustPresent) {

        String scope = options.get(SCOPE_NAME_KEY);
        if (mustPresent.contains(SCOPE_NAME_KEY)) {
            Preconditions.checkNotNull(scope);
        }
        
        String stream = options.get(STREAM_NAME_KEY);
        if (mustPresent.contains(STREAM_NAME_KEY)) {
            Preconditions.checkNotNull(stream);
        }        
        
        String controllerUri = options.get(CONTROLLER_URI_KEY);
        if (mustPresent.contains(CONTROLLER_URI_KEY)) {
            Preconditions.checkNotNull(controllerUri);
        }        
        
        String startStreamcut = options.get(START_STREAMCUT_NAME_KEY);
        if (mustPresent.contains(START_STREAMCUT_NAME_KEY)) {
            Preconditions.checkNotNull(startStreamcut);
        }        
        
        String endStreamcut = options.get(END_STREAMCUT_NAME_KEY);
        if (mustPresent.contains(END_STREAMCUT_NAME_KEY)) {
            Preconditions.checkNotNull(endStreamcut);
        }        
        
        String serializer = options.get(SERIALIZER_CLASS_NAME_KEY);
        if (mustPresent.contains(SERIALIZER_CLASS_NAME_KEY)) {
            Preconditions.checkNotNull(serializer);
        }        
        
        String deserializer = options.get(DESERIALIZER_CLASS_NAME_KEY);
        if (mustPresent.contains(DESERIALIZER_CLASS_NAME_KEY)) {
            Preconditions.checkNotNull(deserializer);
        }

        String truststore = options.get(TRUSTSTORE_FILE_PATH_KEY);
        if (mustPresent.contains(TRUSTSTORE_FILE_PATH_KEY)) {
            Preconditions.checkNotNull(truststore);
        }

        String readerGroupName = READER_GROUP_NAME_DEFAULT;
        if (options.get(READER_GROUP_NAME_KEY) != null) {
            readerGroupName = options.get(READER_GROUP_NAME_KEY);
        }

        String readerGroupScopeName = options.get(READER_GROUP_SCOPE_NAME_KEY);
        if (readerGroupScopeName == null || readerGroupScopeName.length() == 0) {
            readerGroupScopeName = scope;
            Preconditions.checkNotNull(readerGroupScopeName);
        }

        int maxOutstandingCheckpointRequest = MAX_OUTSTANDING_CHECKPOINT_REQUEST_DEFAULT;
        if (options.get(MAX_OUTSTANDING_CHECKPOINT_REQUEST_KEY) != null) {
            maxOutstandingCheckpointRequest = Integer.parseInt(options.get(MAX_OUTSTANDING_CHECKPOINT_REQUEST_KEY));
        }

        long readerGroupRefreshIntervalMillis = READER_GROUP_REFRESH_INTERVAL_MILLIS_DEFAULT;
        if (options.get(READER_GROUP_REFRESH_INTERVAL_MILLIS_KEY) != null) {
            readerGroupRefreshIntervalMillis = Long.parseLong(options.get(READER_GROUP_REFRESH_INTERVAL_MILLIS_KEY));
        }

        long eventReadTimeoutIntervalMillis = EVENT_READ_TIMEOUT_INTERVAL_MILLIS_DEFAULT;
        if (options.get(EVENT_READ_TIMEOUT_INTERVAL_MILLIS_KEY) != null) {
            eventReadTimeoutIntervalMillis = Long.parseLong(options.get(EVENT_READ_TIMEOUT_INTERVAL_MILLIS_KEY));
        }

        long automaticCheckpointIntervalMillis = AUTOMATIC_CHECKPOINT_INTERVAL_MILLIS_DEFAULT;
        if (options.get(AUTOMATIC_CHECKPOINT_INTERVAL_MILLIS_KEY) != null) {
            automaticCheckpointIntervalMillis = Long.parseLong(options.get(AUTOMATIC_CHECKPOINT_INTERVAL_MILLIS_KEY));
        }

        int streamReaderParallelism = STREAM_READER_PARALLELISM_DEFAULT;
        if (options.get(STREAM_READER_PARALLELISM_KEY) != null) {
            streamReaderParallelism = Integer.parseInt(options.get(STREAM_READER_PARALLELISM_KEY));
        }

        return new PravegaInputParams(Stream.of(scope, stream),
                URI.create(controllerUri),
                startStreamcut, endStreamcut,
                serializer, deserializer,
                truststore,
                readerGroupName, readerGroupScopeName, maxOutstandingCheckpointRequest,
                readerGroupRefreshIntervalMillis, eventReadTimeoutIntervalMillis, automaticCheckpointIntervalMillis, streamReaderParallelism);
    }

    public static Set<String> getMandatoryArgs() {
        Set<String> mandatory = new HashSet<>();
        mandatory.add(SCOPE_NAME_KEY);
        mandatory.add(STREAM_NAME_KEY);
        mandatory.add(CONTROLLER_URI_KEY);
        return mandatory;
    }

    @Data
    public static class ReaderGroupInfo {
        private final ReaderGroupConfig readerGroupConfig;
        private final String readerGroupScope;
        private final String readerGroupName;
    }

    public static void main(String ... args) {

        Map<String, String> options = new HashMap<>();
        options.put(SCOPE_NAME_KEY, "scope");
        options.put(STREAM_NAME_KEY, "stream");
        options.put(CONTROLLER_URI_KEY, "tcp://localhost:9090");
        options.put(SERIALIZER_CLASS_NAME_KEY, "io.pravega.client.stream.impl.ByteBufferSerializer");
        options.put(DESERIALIZER_CLASS_NAME_KEY, "io.pravega.client.stream.impl.ByteBufferSerializer");

        Set<String> mandatory = new HashSet<>();
        mandatory.add(SCOPE_NAME_KEY);
        mandatory.add(STREAM_NAME_KEY);
        mandatory.add(CONTROLLER_URI_KEY);
        mandatory.add(SERIALIZER_CLASS_NAME_KEY);
        mandatory.add(DESERIALIZER_CLASS_NAME_KEY);

        PravegaInputParams pravegaInputParams = PravegaInputParams.build(options, mandatory);
        System.out.println(pravegaInputParams.getSerializerInstance());
        System.out.println(pravegaInputParams.getDeSerializerInstance());

        String data = "test";
        Serializer<ByteBuffer> serializer = (Serializer<ByteBuffer>) pravegaInputParams.getSerializerInstance();
        ByteBuffer s = serializer.serialize(ByteBuffer.wrap(data.getBytes()));
        byte[] buffer = new byte[s.capacity()];
        serializer.deserialize(s).get(buffer, 0, s.capacity());
        String val = new String(buffer);
        System.out.println(val);

    }

}
