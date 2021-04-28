/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.UnionLogicalType;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.readRecord;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.readRecordFromAvroFile;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecordToAvroFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link AvroDeserializationSchema}. */
public class AvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @Test
    public void testNullRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        Address deserializedAddress = deserializer.deserialize(null);
        assertNull(deserializedAddress);
    }

    @Test
    public void testEvolution() throws Exception {
        Schema schema1 =
                Schema.parse(
                        "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                + " \"type\": \"record\",\n"
                                + " \"name\": \"Address\",\n"
                                + " \"fields\": [\n"
                                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                + "     {\"name\": \"city\", \"type\": \"string\"}\n"
                                + "  ]\n"
                                + "}");

        GenericRecord record = new GenericData.Record(schema1);
        record.put("id", 1);
        record.put("city", "Shanghai");
        byte[] bytes = writeRecord(record, schema1);

        Schema schema2 =
                Schema.parse(
                        "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                + " \"type\": \"record\",\n"
                                + " \"name\": \"Address\",\n"
                                + " \"fields\": [\n"
                                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                + "     {\"name\": \"city\", \"type\": \"string\"},\n"
                                + "     {\"name\": \"state\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
                                + "  ]\n"
                                + "}");
        GenericRecord record1 = readRecord(bytes, schema1, schema2);
        System.out.println(record1);
    }

    @Test
    public void testAvroFileEvolution() throws Exception {
        Schema schema1 =
                Schema.parse(
                        "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                + " \"type\": \"record\",\n"
                                + " \"name\": \"Address\",\n"
                                + " \"fields\": [\n"
                                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                + "     {\"name\": \"city\", \"type\": \"string\"}\n"
                                + "  ]\n"
                                + "}");

        // add nullable column
        Schema schema2 =
                Schema.parse(
                        "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                + " \"type\": \"record\",\n"
                                + " \"name\": \"Address\",\n"
                                + " \"fields\": [\n"
                                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                + "     {\"name\": \"state\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
                                + "     {\"name\": \"city\", \"type\": \"string\"}\n"
                                + "  ]\n"
                                + "}");

        // add not-null column
        Schema schema3 =
                Schema.parse(
                        "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                + " \"type\": \"record\",\n"
                                + " \"name\": \"Address\",\n"
                                + " \"fields\": [\n"
                                + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                                + "     {\"name\": \"state\", \"type\": \"string\"},\n"
                                + "     {\"name\": \"city\", \"type\": \"string\"}\n"
                                + "  ]\n"
                                + "}");

        final GenericRecord record1 = new GenericData.Record(schema1);
        record1.put("id", 1);
        record1.put("city", "Shanghai");

        final GenericRecord record2 = new GenericData.Record(schema2);
        record2.put("id", 1);
        record2.put("state", "Zhejiang");
        record2.put("city", "Shanghai");

        final GenericRecord record3 = new GenericData.Record(schema2);
        record3.put("id", 1);
        record3.put("state", "Zhejiang");
        record3.put("city", "Shanghai");

        // ===== add nullable column (schema1 -> schema2) ========
        System.out.println("old schema writer, new schema reader, should pass");
        verifyAvroFile(schema1, schema2, record1);
        System.out.println("new schema writer, old schema reader, should pass");
        verifyAvroFile(schema2, schema1, record2);

        // ====== add not null column (schema1 -> schema3) ========
        System.out.println("old schema writer, new schema reader, should fail");
        try {
            verifyAvroFile(schema1, schema3, record1);
            fail("can't pass");
        } catch (AvroTypeException e) {
            // ignore
            System.out.println("failed to deserialize");
        }
        System.out.println("new schema writer, old schema reader, should pass");
        verifyAvroFile(schema3, schema1, record3);

        // ====== remove nullable column (schema2 -> schema1) ======
        System.out.println("old schema writer, new schema reader, should pass");
        verifyAvroFile(schema2, schema1, record2);
        System.out.println("new schema writer, old schema reader, should pass");
        verifyAvroFile(schema1, schema2, record1);

        // ====== remove not-null column (schema3 -> schema1) ======
        System.out.println("old schema writer, new schema reader, should pass");
        verifyAvroFile(schema3, schema1, record3);
        System.out.println("new schema writer, old schema reader, should fail");
        try {
            verifyAvroFile(schema1, schema3, record1);
            fail("can't pass");
        } catch (AvroTypeException e) {
            // ignore
            System.out.println("failed to deserialize");
        }
    }

    private void verifyAvroFile(Schema writeSchema, Schema readSchema, GenericRecord... records)
            throws IOException {
        String path = writeRecordToAvroFile(writeSchema, records);
        List<GenericRecord> readRecords = readRecordFromAvroFile(path, readSchema);
        System.out.println(readRecords);
    }

    @Test
    public void testGenericRecord() throws Exception {
        DeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(address.getSchema());

        byte[] encodedAddress = writeRecord(address, Address.getClassSchema());
        GenericRecord genericRecord = deserializationSchema.deserialize(encodedAddress);
        assertEquals(address.getCity(), genericRecord.get("city").toString());
        assertEquals(address.getNum(), genericRecord.get("num"));
        assertEquals(address.getState(), genericRecord.get("state").toString());
    }

    @Test
    public void testSpecificRecord() throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class);

        byte[] encodedAddress = writeRecord(address);
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertEquals(address, deserializedAddress);
    }

    @Test
    public void testSpecificRecordWithUnionLogicalType() throws Exception {
        Random rnd = new Random();
        UnionLogicalType data = new UnionLogicalType(Instant.ofEpochMilli(rnd.nextLong()));
        DeserializationSchema<UnionLogicalType> deserializer =
                AvroDeserializationSchema.forSpecific(UnionLogicalType.class);

        byte[] encodedData = writeRecord(data);
        UnionLogicalType deserializedData = deserializer.deserialize(encodedData);
        assertEquals(data, deserializedData);
    }
}
