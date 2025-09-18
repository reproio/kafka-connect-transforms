package com.reproio.kafka.connect.transforms;

import static org.junit.jupiter.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class AvroMapToJsonStringTest {

  @Test
  void transformsValue_UnionMap() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("bool", false);
    props.put("int", 1);
    props.put("long", 123L);
    props.put("double", 1.23);
    props.put("string", "X");
    props.put("null", null);

    var value = inputUnionValue(props);
    var valueSchema = inputValueSchema(unionSchema());

    var in = new SinkRecord("topic-a", 0, null, null, valueSchema, value, 123L);

    AvroMapToJsonString.Value<SinkRecord> transformer = new AvroMapToJsonString.Value<>();
    transformer.configure(Map.of("field", "properties"));

    var out = transformer.apply(in);
    assertNotNull(out);
    assertNotNull(out.valueSchema());
    assertInstanceOf(Struct.class, out.value());

    var outStruct = (Struct) out.value();
    var propertiesField = out.valueSchema().field("properties");
    assertNotNull(propertiesField);
    assertEquals(Schema.Type.STRING, propertiesField.schema().type());

    var expectedString =
        """
        {
          "bool": false,
          "int": 1,
          "long": 123,
          "double": 1.23,
          "string": "X",
          "null": null
        }
        """
            .replaceAll("\\s+", "");
    assertEquals(expectedString, outStruct.get("properties"));
  }

  @Test
  void transformsValue_StringMap() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put("key-1", "value-1");
    props.put("key-2", "value-2");
    props.put("key-3", "value-3");

    var valueSchema = inputValueSchema(Schema.STRING_SCHEMA);
    var value = new Struct(valueSchema);
    value.put("properties", new LinkedHashMap<>(props));

    var in = new SinkRecord("topic-a", 0, null, null, valueSchema, value, 123L);

    AvroMapToJsonString.Value<SinkRecord> transformer = new AvroMapToJsonString.Value<>();
    transformer.configure(Map.of("field", "properties"));

    var out = transformer.apply(in);
    assertNotNull(out);
    assertNotNull(out.valueSchema());
    assertInstanceOf(Struct.class, out.value());

    var outStruct = (Struct) out.value();
    var propertiesField = out.valueSchema().field("properties");
    assertNotNull(propertiesField);
    assertEquals(Schema.Type.STRING, propertiesField.schema().type());

    var expectedString =
        """
        {
          "key-1": "value-1",
          "key-2": "value-2",
          "key-3": "value-3"
        }
        """
            .replaceAll("\\s+", "");
    assertEquals(expectedString, outStruct.get("properties"));
  }

  private static Schema inputValueSchema(Schema mapValueSchema) {
    return SchemaBuilder.struct()
        .name("top.schema")
        .version(1)
        .field(
            "properties",
            SchemaBuilder.map(Schema.STRING_SCHEMA, mapValueSchema).optional().build())
        .build();
  }

  private static Struct inputUnionValue(Map<String, Object> propsEntries) {
    var vs = inputValueSchema(unionSchema());
    var st = new Struct(vs);
    Map<String, Object> m = new LinkedHashMap<>();
    for (var e : propsEntries.entrySet()) {
      m.put(e.getKey(), unionOf(e.getValue()));
    }
    st.put("properties", m);
    return st;
  }

  private static Schema unionSchema() {
    return SchemaBuilder.struct()
        .name("union.like")
        .optional()
        .field("string", Schema.OPTIONAL_STRING_SCHEMA)
        .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("int", Schema.OPTIONAL_INT32_SCHEMA)
        .field("long", Schema.OPTIONAL_INT64_SCHEMA)
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("bytes", Schema.OPTIONAL_BYTES_SCHEMA)
        .build();
  }

  private static Struct unionOf(Object v) {
    Schema us = unionSchema();
    Struct s = new Struct(us);
    if (v == null) {
      return s;
    }
    if (v instanceof CharSequence cs) return s.put("string", cs.toString());
    if (v instanceof Boolean b) return s.put("boolean", b);
    if (v instanceof Integer i) return s.put("int", i);
    if (v instanceof Long l) return s.put("long", l);
    if (v instanceof Double d) return s.put("double", d);
    if (v instanceof Float f) return s.put("float", f);
    if (v instanceof byte[] bytes) return s.put("bytes", bytes);
    return s.put("string", String.valueOf(v));
  }
}
