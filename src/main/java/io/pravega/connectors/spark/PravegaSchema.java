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

import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PravegaSchema {

    public static StructType readSchema() {
        StructField[] structFields = new StructField[5];
        structFields[0] = new StructField("event", BinaryType$.MODULE$, false, Metadata.empty());
        structFields[1] = new StructField("scope", StringType$.MODULE$, false, Metadata.empty());
        structFields[2] = new StructField("stream", StringType$.MODULE$, false, Metadata.empty());
        structFields[3] = new StructField("segmentId", LongType$.MODULE$, false, Metadata.empty());
        structFields[4] = new StructField("OFFSET", LongType$.MODULE$, false, Metadata.empty());
        return new StructType(structFields);
    }
}
