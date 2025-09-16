package com.reproio.kafka.connect.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class MapToJsonString<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELD_CONFIG = "field";
  public static final ObjectMapper mapper = new ObjectMapper();
  private String fieldName;

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "map field name");

  @Override
  public void configure(Map<String, ?> props) {
    final AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
    fieldName = config.getString(FIELD_CONFIG);
  }

  protected MapToJsonString() {}

  @Override
  public R apply(R record) {
    if (operatingValue(record) == null) {
      return record;
    }

    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  public R applySchemaless(R record) {
    return record;
  }

  public R applyWithSchema(R record) {
    var schema = operatingSchema(record);
    var value = record.value();

    if (schema == null || !(value instanceof Struct v)) return record;

    var schemaBuilder = SchemaBuilder.struct().name(schema.name()).version(schema.version());
    if (schema.doc() != null) schemaBuilder.doc(schema.doc());

    Map<String, Object> newVals = new HashMap<>();
    for (var field : schema.fields()) {
      if (!field.name().equals(fieldName)) {
        schemaBuilder.field(field.name(), field.schema());
        newVals.put(field.name(), v.get(field));
        continue;
      }

      schemaBuilder.field(field.name(), Schema.OPTIONAL_STRING_SCHEMA);
      Object mapObj = v.get(field);
      newVals.put(field.name(), mapObj instanceof Map<?, ?> m ? toJsonString(m) : null);
    }

    var newSchema = schemaBuilder.build();
    var out = new Struct(newSchema);
    for (var f : newSchema.fields()) out.put(f, newVals.get(f.name()));
    return newRecord(record, newSchema, out);
  }

  private String toJsonString(Map<?, ?> m) {
    Map<String, Object> plain = new LinkedHashMap<>();
    for (var e : m.entrySet()) {
      plain.put(String.valueOf(e.getKey()), unwrapUnion(e.getValue()));
    }
    try {
      return mapper.writeValueAsString(plain);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object unwrapUnion(Object u) {
    if (u == null) return null;
    if (u instanceof Struct s) {
      for (String k :
          new String[] {"string", "boolean", "int", "long", "double", "float", "bytes"}) {
        try {
          Object v = s.get(k);
          if (v != null) return v;
        } catch (Exception ignore) {
        }
      }
      return null;
    }
    return u;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends MapToJsonString<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          updatedSchema,
          updatedValue,
          record.valueSchema(),
          record.value(),
          record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends MapToJsonString<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          updatedSchema,
          updatedValue,
          record.timestamp());
    }
  }
}
