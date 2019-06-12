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

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.spark.PravegaSourceProvider;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
//import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static io.pravega.connectors.spark.PravegaInputParams.CONTROLLER_URI_KEY;
import static io.pravega.connectors.spark.PravegaInputParams.EVENT_ATTRIBUTE_NAME;
//import static io.pravega.connectors.spark.PravegaInputParams.ROUTING_KEY_ATTRIBUTE_NAME;
//import static io.pravega.connectors.spark.PravegaInputParams.READER_GROUP_NAME_KEY;
import static io.pravega.connectors.spark.PravegaInputParams.SCOPE_NAME_KEY;
import static io.pravega.connectors.spark.PravegaInputParams.STREAM_NAME_KEY;
import static io.pravega.connectors.spark.PravegaInputParams.STREAM_READER_PARALLELISM_KEY;

public class TestMain {

    static final String CONTROLLER_URI = "tcp://localhost:9090";

    public static void createStream(String streamName) {
        SetupUtils setupUtils = new SetupUtils(CONTROLLER_URI);
        try {
            setupUtils.startAllServices();

            //scope = TjLdpRVaukYfIrzWAbXU
            setupUtils.createTestStream(streamName, 3);
            System.out.println("created " + setupUtils.getScope() + "/" + streamName);

            setupUtils.stopAllServices();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeStringsToStream(Stream stream) {

        SparkSession spark = SparkSession.builder().appName("writer").master("local[*]")
                .getOrCreate();

        List<byte[]> stringAsList = new ArrayList<>();
        stringAsList.add("foo1".getBytes());
        stringAsList.add("bar1".getBytes());
        stringAsList.add("bar2".getBytes());
        stringAsList.add("bar3".getBytes());

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((byte[] row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(EVENT_ATTRIBUTE_NAME, DataTypes.BinaryType, false) });
        //DataTypes.createStructField(ROUTING_KEY_ATTRIBUTE_NAME, DataTypes.StringType, false),

        Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();

        df.write()
                .format(PravegaSourceProvider.ALIAS)
                .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                .option(SCOPE_NAME_KEY, stream.getScope())
                .option(STREAM_NAME_KEY, stream.getStreamName())
                //.mode(SaveMode.Append)
                .save();
    }

    public static void readStringsFromStream(Stream stream) {
        SparkSession spark = SparkSession.builder().appName("reader").master("local[*]")
                .getOrCreate();

        //DeSerializerUDF deSerializerUDF = new DeSerializerUDF();
        //spark.udf().register("deserialize", deSerializerUDF, DataTypes.BinaryType);

        Dataset<Row> ds =
        spark.readStream()
                .format(PravegaSourceProvider.ALIAS)
                .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                .option(SCOPE_NAME_KEY, stream.getScope())
                .option(STREAM_NAME_KEY, stream.getStreamName())
                .load();

        ds = ds.selectExpr("CAST(event AS STRING) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");
        //ds = ds.selectExpr("deserialize(event) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");
        //ds = ds.selectExpr("CAST(scope AS STRING) As scope", "CAST(stream AS STRING) As stream");

        ds.printSchema();
        //ds.write().mode("append").format("console").save();

        StreamingQuery query0 = ds.writeStream().outputMode("update").format("console").start();
        try {
            query0.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

    public static void writeEventsToStream(int totalEvents, boolean useStreamWriter, Stream stream) {

        SparkSession spark = SparkSession.builder().appName("writer").master("local[*]")
                .getOrCreate();

        List<byte[]> eventList = new ArrayList<>();
        JavaSerializer<Event> serializer = new JavaSerializer<>();
        for (int i = 1; i <= totalEvents; i++) {
            Event event = new Event("event-" + i, i);

            ByteBuffer buf = serializer.serialize(event);
            byte[] bytes;

            if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
                bytes = buf.array();
            } else {
                bytes = new byte[buf.remaining()];
                buf.get(bytes);
            }
            eventList.add(bytes);
        }

        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Row> rowRDD = sparkContext.parallelize(eventList).map((byte[] row) -> RowFactory.create(row));

        // Creates schema
        StructType schema = DataTypes
                .createStructType(new StructField[] {
                        DataTypes.createStructField(EVENT_ATTRIBUTE_NAME, DataTypes.BinaryType, false) });
        //DataTypes.createStructField(ROUTING_KEY_ATTRIBUTE_NAME, DataTypes.StringType, false),

        if (useStreamWriter) {
            Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
            df.writeStream()
                    .format(PravegaSourceProvider.ALIAS)
                    .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                    .option(SCOPE_NAME_KEY, stream.getScope())
                    .option(STREAM_NAME_KEY, stream.getStreamName())
                    //.mode(SaveMode.Append)
                    .start();
        } else {
            Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
            df.write()
                    .format(PravegaSourceProvider.ALIAS)
                    .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                    .option(SCOPE_NAME_KEY, stream.getScope())
                    .option(STREAM_NAME_KEY, stream.getStreamName())
                    //.mode(SaveMode.Append)
                    .save();
        }
    }

    public static void readEventsFromStream1(Stream stream) {
        SparkSession spark = SparkSession.builder().appName("reader").master("local[*]")
                .getOrCreate();

        EventDeSerializerUDF eventDeSerializerUDF = new EventDeSerializerUDF();
        //spark.udf().register("deserialize", eventDeSerializerUDF, DataTypes.BinaryType);

        Dataset<Row> ds =
                spark.read()
                        .format(PravegaSourceProvider.ALIAS)
                        .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                        .option(SCOPE_NAME_KEY, stream.getScope())
                        .option(STREAM_NAME_KEY, stream.getStreamName())
                        .load();

        //ds = ds.selectExpr("deserialize(event) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");
        //ds = ds.selectExpr("deserialize(event) AS event");

        ds.printSchema();

        // not useful
        //ds.withColumn("event", functions.callUDF("deserialize", functions.col("event")));

        //not working
        ds.map(new MapFunction<Row, Result>() {
            @Override
            public Result call(Row value) throws Exception {
                JavaSerializer<Event> serializer = new JavaSerializer<>();
                byte[] bytes = (byte[]) value.get(0);
                Event event = serializer.deserialize(ByteBuffer.wrap(bytes));
                String scope = value.getString(1);
                String stream = value.getString(2);
                long segmentId = value.getLong(3);
                long offset = value.getLong(4);
                return new Result(event, scope, stream, segmentId, offset);
            }
        }, Encoders.bean(Result.class)).show();
        //.javaSerialization(Result.class))

        //ds.write().mode("append").format("console").save();

        /*

        StreamingQuery query0 = ds.writeStream().outputMode("update").format("console").start();
        try {
            query0.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

         */

    }

    public static void readEventsFromStream2(Stream stream) {
        SparkSession spark = SparkSession.builder().appName("reader").master("local[*]")
                .getOrCreate();

        EventDeSerializerUDF eventDeSerializerUDF = new EventDeSerializerUDF();
        spark.udf().register("deserialize", eventDeSerializerUDF, DataTypes.BinaryType);

        Dataset<Row> ds =
                spark.readStream()
                        .format(PravegaSourceProvider.ALIAS)
                        .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                        .option(SCOPE_NAME_KEY, stream.getScope())
                        .option(STREAM_NAME_KEY, stream.getStreamName())
                        .load();

        ds = ds.selectExpr("deserialize(event) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");

        ds.printSchema();

        //ds.write().mode("append").format("console").save();

        StreamingQuery query0 = ds.writeStream().outputMode("update").format("console").start();
        try {
            query0.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

    public static void readContinuousEventsFromStream(Stream stream) {
        SparkSession spark = SparkSession.builder().appName("reader").master("local[*]")
                .getOrCreate();

        EventDeSerializerUDF eventDeSerializerUDF = new EventDeSerializerUDF();
        spark.udf().register("deserialize", eventDeSerializerUDF, DataTypes.BinaryType);

        Dataset<Row> ds =
                spark.readStream()
                        .format(PravegaSourceProvider.ALIAS)
                        .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                        .option(SCOPE_NAME_KEY, stream.getScope())
                        .option(STREAM_NAME_KEY, stream.getStreamName())
                        .option(STREAM_READER_PARALLELISM_KEY, 3)
                        //.option(READER_GROUP_NAME_KEY, "foo1")
                        .load();

        ds = ds.selectExpr("deserialize(event) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");

        ds.printSchema();

        //ds.write().mode("append").format("console").save();

        StreamingQuery query = ds.writeStream().outputMode("update").format("console").trigger(Trigger.Continuous(10000)).start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

    public static void readContinuousEventsFromStreamAndWriteToPravega(Stream stream, Stream outputStream) {
        SparkSession spark = SparkSession.builder().appName("reader").master("local[*]")
                .getOrCreate();

        EventDeSerializerUDF eventDeSerializerUDF = new EventDeSerializerUDF();
        EventSerializerUDF eventSerializerUDF = new EventSerializerUDF();
        spark.udf().register("deserialize", eventDeSerializerUDF, DataTypes.BinaryType);
        //spark.udf().register("serialize", eventSerializerUDF, DataTypes.BinaryType);

        Dataset<Row> ds =
                spark.readStream()
                        .format(PravegaSourceProvider.ALIAS)
                        .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                        .option(SCOPE_NAME_KEY, stream.getScope())
                        .option(STREAM_NAME_KEY, stream.getStreamName())
                        .option(STREAM_READER_PARALLELISM_KEY, 3)
                        //.option(READER_GROUP_NAME_KEY, "foo1")
                        .load();

        //ds = ds.selectExpr("deserialize(event) AS event", "CAST(scope AS STRING) as scope", "CAST(stream AS STRING) as stream", "CAST(segmentId AS Long) as segmentId", "CAST(OFFSET AS Long) as OFFSET");
        ds = ds.selectExpr("deserialize(event) AS event");
        //ds = ds.selectExpr("serialize(event) AS event");

        ds.printSchema();

        StreamingQuery query =
                ds.writeStream()
                .format(PravegaSourceProvider.ALIAS)
                .option(CONTROLLER_URI_KEY, CONTROLLER_URI)
                .option(SCOPE_NAME_KEY, outputStream.getScope())
                .option(STREAM_NAME_KEY, outputStream.getStreamName())
                .outputMode("append")
                .option("checkpointLocation", "/tmp/" + UUID.randomUUID().toString())
                .trigger(Trigger.Continuous(10000))
                .start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }

    @Data
    @AllArgsConstructor
    @ToString
    public static final class Event implements Serializable {
        private String type;
        private int value;
    }

    @Data
    @AllArgsConstructor
    @ToString
    public static final class Result implements Serializable {
        private Event event;
        private String scope;
        private String stream;
        private long segmentId;
        private long offset;
    }

    static class DeSerializerUDF implements UDF1<byte[], String> {
        @Override
        public String call(byte[] bytes) throws Exception {
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    @Slf4j
    static class EventDeSerializerUDF implements UDF1<byte[], byte[]> {
        JavaSerializer<Event> serializer = new JavaSerializer<>();
        @Override
        public byte[] call(byte[] bytes) throws Exception {
            Event event = serializer.deserialize(ByteBuffer.wrap(bytes));
            //return event.toString().getBytes();

            ByteBuffer buf = serializer.serialize(event);
            byte[] bytes1;

            if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
                bytes1 = buf.array();
            } else {
                bytes1 = new byte[buf.remaining()];
                buf.get(bytes1);
            }
            return bytes1;
        }
    }

    @Slf4j
    static class EventSerializerUDF implements UDF1<Event, byte[]> {
        JavaSerializer<Event> serializer = new JavaSerializer<>();
        @Override
        public byte[] call(Event event) throws Exception {
            ByteBuffer buf = serializer.serialize(event);
            byte[] bytes;

            if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
                bytes = buf.array();
            } else {
                bytes = new byte[buf.remaining()];
                buf.get(bytes);
            }
            return bytes;
        }
    }

    public static void main(String... args) {

        String inputScope = "uKCdAGQNUjPxloICoNfs";
        String outputScope = "gChXrXYIVogeQMtucvfV";
        String inputStreamName = "testIn";
        String outputStreamName = "testOut";

        Stream inputStream = Stream.of(inputScope, inputStreamName);
        Stream outputStream = Stream.of(outputScope, outputStreamName);

        //createStream(inputStreamName);
        //createStream(outputStreamName);

        //writeStringsToStream(inputStream);
        //readStringsFromStream(inputStream);

        //writeEventsToStream(1, false, inputStream);
        //readEventsFromStream1(inputStream);
        //readEventsFromStream2(inputStream);

        //writeEventsToStream(5, false, inputStream);
        //readContinuousEventsFromStream(inputStream);

        //writeEventsToStream(5, false, inputStream);
        //readContinuousEventsFromStreamAndWriteToPravega(inputStream, outputStream);
        readContinuousEventsFromStream(outputStream);
    }
}
