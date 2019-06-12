/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.spark.utils;

import io.pravega.client.stream.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class StringSerializer implements Serializer<String> {
    @Override
    public ByteBuffer serialize(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

    @Override
    public String deserialize(ByteBuffer serializedValue) {
        return StandardCharsets.UTF_8.decode(serializedValue).toString();
    }
}
