package io.github.richardstartin.bson.analysis;

import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.PrintStream;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.bson.BsonType.*;

public class BsonOverheadAnalyser implements BsonWriter {

  private EnumMap<BsonType, Integer> sizeForDataByType = new EnumMap<>(BsonType.class);
  private EnumMap<BsonType, Integer> sizeForTypeMarkersByType = new EnumMap<>(BsonType.class);
  private Map<String, Integer> sizeForAttributesByAttribute = new TreeMap<>();
  private int sizeForNullTerminators;
  private int sizeForBinaryTypeMarkers;
  private int sizeForDocumentLengths;
  private Map<String, Integer> sizeForDocumentLengthsByAttribute = new TreeMap<>();
  private Map<String, Integer> sizeForDataByAttribute = new TreeMap<>();
  private int documentSize;

  private final Stack<Context> contexts = new Stack<>();

  public BsonOverheadAnalyser() {
    contexts.push(new Context("root"));
    contexts.peek().contributeSize(4 + 1); // doc length + null terminator
    sizeForDocumentLengthsByAttribute.put("root", 4);
    sizeForDocumentLengths += 4;
  }


  private class Context {
    int sizeCovered;
    final String attributeName;
    boolean isArray;

    private Context(String attributeName) {
      this (attributeName, false);
    }

    private Context(String attributeName, boolean isArray) {
      this.attributeName = attributeName;
      this.isArray = isArray;
    }

    void contributeSize(int size) {
      if (isArray) {
        contexts.pop();
        contexts.peek().contributeSize(size);
        contexts.push(this);
      } else {
        sizeCovered += size;
      }
    }
  }

  public void printStatistics(PrintStream out) {
    out.format("+----------------------------------------------+\n");
    out.format("data size by type (total): %dB\n", sizeForDataByType.values().stream().mapToInt(Integer::intValue).sum());
    for (var entry : sizeForDataByType.entrySet()) {
      out.format("\t %s: %dB\n", entry.getKey().name(), entry.getValue());
    }
    out.format("binary type markers (total): %dB\n", sizeForBinaryTypeMarkers);
    out.format("+----------------------------------------------+\n");
    out.format("attribute (total): %dB\n", sizeForAttributesByAttribute.values().stream().mapToInt(Integer::intValue).sum());
    out.format("null terminators: %dB\n", sizeForNullTerminators);
    out.format("+----------------------------------------------+\n");
    out.format("document lengths (total): %dB\n", sizeForDocumentLengths);
    for (var entry : sizeForDocumentLengthsByAttribute.entrySet()) {
      out.format("\t%s: %dB\n", entry.getKey(), entry.getValue());
    }
    out.format("+----------------------------------------------+\n");
    out.format("data (total): %dB\n", sizeForDataByAttribute.values().stream().mapToInt(Integer::intValue).sum());
    for (var entry : sizeForDataByAttribute.entrySet()) {
      out.format("\t%s: %dB\n", entry.getKey(), entry.getValue());
    }
    out.format("\t-------------------------------------------+\n");
    for (var entry : sizeForDataByType.entrySet()) {
      out.format("\t%s: %dB\n", entry.getKey().name(), entry.getValue());
    }
    out.format("+----------------------------------------------+\n");
    int dataSize = sizeForDataByAttribute.values().stream().mapToInt(Integer::intValue).sum();
    out.format("document size (total): %dB\n", documentSize);
    out.format("Overhead: %.2f%%", (100D * (documentSize - dataSize)) / documentSize);
  }

  @Override
  public void flush() {

  }

  @Override
  public void writeBinaryData(BsonBinary bsonBinary) {

  }

  private void recordSize(String attribute, BsonType type, int dataSize) {
    ++sizeForNullTerminators;
    Context ctx = contexts.peek();
    sizeForDataByAttribute.compute(ctx.isArray ? ctx.attributeName : attribute, (k, v) -> (null == v ? 0 : v) + dataSize);
    sizeForDataByType.compute(type, (t, v) -> (null == v ? 0 : v) + dataSize);
    sizeForTypeMarkersByType.compute(type, (t, v) -> (null == v ? 0 : v) + 1);
    sizeForAttributesByAttribute.compute(attribute, (a, v) -> (null == v ? 0 : v) + a.getBytes(UTF_8).length);
    contexts.peek().contributeSize(attribute.getBytes(UTF_8).length + 1 + 1 + dataSize);
  }

  @Override
  public void writeBinaryData(String s, BsonBinary bsonBinary) {
    ++sizeForBinaryTypeMarkers;
    contexts.peek().contributeSize(1);
    recordSize(s, BINARY, bsonBinary.getData().length + 4);
  }

  @Override
  public void writeBoolean(boolean b) {

  }

  @Override
  public void writeBoolean(String s, boolean b) {
    recordSize(s, BOOLEAN, 1);
  }

  @Override
  public void writeDateTime(long l) {

  }

  @Override
  public void writeDateTime(String s, long l) {
    recordSize(s, DATE_TIME, 8);
  }

  @Override
  public void writeDBPointer(BsonDbPointer bsonDbPointer) {

  }

  @Override
  public void writeDBPointer(String s, BsonDbPointer bsonDbPointer) {
    recordSize(s, DB_POINTER, 12);
  }

  @Override
  public void writeDouble(double v) {

  }

  @Override
  public void writeDouble(String s, double v) {
    recordSize(s, DOUBLE, 8);
  }

  @Override
  public void writeEndArray() {
    contexts.pop();
  }

  @Override
  public void writeEndDocument() {
    Context popped = contexts.pop();
    ++sizeForNullTerminators;
    if (!contexts.isEmpty()) {
      contexts.peek().contributeSize(popped.sizeCovered);
    } else {
      documentSize = popped.sizeCovered;
    }
  }

  @Override
  public void writeInt32(int i) {

  }

  @Override
  public void writeInt32(String s, int i) {
    recordSize(s, INT32, 4);
  }

  @Override
  public void writeInt64(long l) {

  }

  @Override
  public void writeInt64(String s, long l) {
    recordSize(s, INT64, 8);
  }

  @Override
  public void writeDecimal128(Decimal128 decimal128) {

  }

  @Override
  public void writeDecimal128(String s, Decimal128 decimal128) {
    recordSize(s, DECIMAL128, 16);
  }

  @Override
  public void writeJavaScript(String s) {

  }

  @Override
  public void writeJavaScript(String s, String s1) {
    recordSize(s, JAVASCRIPT, 4 + s1.getBytes(UTF_8).length);
  }

  @Override
  public void writeJavaScriptWithScope(String s) {

  }

  @Override
  public void writeJavaScriptWithScope(String s, String s1) {
    recordSize(s, JAVASCRIPT_WITH_SCOPE, 4 + s1.getBytes(UTF_8).length);
  }

  @Override
  public void writeMaxKey() {

  }

  @Override
  public void writeMaxKey(String s) {
    recordSize(s, MAX_KEY, 0);
  }

  @Override
  public void writeMinKey() {

  }

  @Override
  public void writeMinKey(String s) {
    recordSize(s, MIN_KEY, 0);
  }

  @Override
  public void writeName(String s) {

  }

  @Override
  public void writeNull() {
  }

  @Override
  public void writeNull(String s) {
    ++sizeForNullTerminators;
    recordSize(s, NULL, 0);
  }

  @Override
  public void writeObjectId(ObjectId objectId) {

  }

  @Override
  public void writeObjectId(String s, ObjectId objectId) {
    recordSize(s, OBJECT_ID, 12);
  }

  @Override
  public void writeRegularExpression(BsonRegularExpression bsonRegularExpression) {

  }

  @Override
  public void writeRegularExpression(String s, BsonRegularExpression bsonRegularExpression) {
    recordSize(s, REGULAR_EXPRESSION,
            bsonRegularExpression.getPattern().getBytes(UTF_8).length + 1
                    + bsonRegularExpression.getOptions().getBytes(UTF_8).length + 1);
  }

  @Override
  public void writeStartArray() {

  }

  @Override
  public void writeStartArray(String s) {
    sizeForNullTerminators += 2; // (array and cstring)
    sizeForDocumentLengths += 4; // (arrays are defined as documents
    sizeForAttributesByAttribute.compute(s, (k, v) -> (null == v ? 0 : v) + k.getBytes(UTF_8).length);
    sizeForDocumentLengthsByAttribute.compute(s, (k, v) -> (null == v ? 0 : v) + 4);
    // type byte + string + null terminator + doc length + null terminator
    contexts.peek().contributeSize(s.getBytes(UTF_8).length + 1 + 1 + 4 + 1);
    contexts.push(new Context(s, true));
  }

  @Override
  public void writeStartDocument() {

  }

  @Override
  public void writeStartDocument(String s) {
    Context ctx = contexts.peek();
    contexts.push(new Context(s));
    ++sizeForNullTerminators;
    sizeForAttributesByAttribute.compute(s, (k, v) -> (null == v ? 0 : v) + k.getBytes(UTF_8).length);
    sizeForDocumentLengths += 4;
    sizeForDocumentLengthsByAttribute.compute(ctx.isArray ? ctx.attributeName : s, (k, v) -> (v == null ? 0 : v) + 4);
    contexts.peek().contributeSize(s.getBytes(UTF_8).length + 1 + 4 + 1 + 1);
  }

  @Override
  public void writeString(String s) {

  }

  @Override
  public void writeString(String s, String s1) {
    ++sizeForNullTerminators;
    recordSize(s, STRING, 4 + s1.getBytes(UTF_8).length + 1);
  }

  @Override
  public void writeSymbol(String s) {

  }

  @Override
  public void writeSymbol(String s, String s1) {

  }

  @Override
  public void writeTimestamp(BsonTimestamp bsonTimestamp) {

  }

  @Override
  public void writeTimestamp(String s, BsonTimestamp bsonTimestamp) {
    recordSize(s, TIMESTAMP, 8);
  }

  @Override
  public void writeUndefined() {

  }

  @Override
  public void writeUndefined(String s) {

  }

  @Override
  public void pipe(final BsonReader reader) {
    pipeDocument(null, reader);
  }

  private void pipeDocument(String attribute, final BsonReader reader) {
    reader.readStartDocument();
    if (null == attribute) {
      writeStartDocument();
    } else {
      writeStartDocument(attribute);
    }
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      pipeValue(reader.readName(), reader);
    }
    reader.readEndDocument();
    writeEndDocument();
  }

  private void pipeJavascriptWithScope(String attribute, BsonReader reader) {
    writeJavaScriptWithScope(attribute, reader.readJavaScriptWithScope());
    pipeDocument(attribute, reader);
  }

  private void pipeValue(String attribute, BsonReader reader) {
    switch (reader.getCurrentBsonType()) {
      case DOCUMENT:
        pipeDocument(attribute, reader);
        break;
      case ARRAY:
        pipeArray(attribute, reader);
        break;
      case DOUBLE:
        writeDouble(attribute, reader.readDouble());
        break;
      case STRING:
        writeString(attribute, reader.readString());
        break;
      case BINARY:
        writeBinaryData(attribute, reader.readBinaryData());
        break;
      case UNDEFINED:
        reader.readUndefined();
        writeUndefined();
        break;
      case OBJECT_ID:
        writeObjectId(attribute, reader.readObjectId());
        break;
      case BOOLEAN:
        writeBoolean(attribute, reader.readBoolean());
        break;
      case DATE_TIME:
        writeDateTime(attribute, reader.readDateTime());
        break;
      case NULL:
        reader.readNull();
        writeNull(attribute);
        break;
      case REGULAR_EXPRESSION:
        writeRegularExpression(attribute, reader.readRegularExpression());
        break;
      case JAVASCRIPT:
        writeJavaScript(attribute, reader.readJavaScript());
        break;
      case SYMBOL:
        writeSymbol(attribute, reader.readSymbol());
        break;
      case JAVASCRIPT_WITH_SCOPE:
        pipeJavascriptWithScope(attribute, reader);
        break;
      case INT32:
        writeInt32(attribute, reader.readInt32());
        break;
      case TIMESTAMP:
        writeTimestamp(attribute, reader.readTimestamp());
        break;
      case INT64:
        writeInt64(attribute, reader.readInt64());
        break;
      case DECIMAL128:
        writeDecimal128(attribute, reader.readDecimal128());
        break;
      case MIN_KEY:
        reader.readMinKey();
        writeMinKey(attribute);
        break;
      case DB_POINTER:
        writeDBPointer(attribute, reader.readDBPointer());
        break;
      case MAX_KEY:
        reader.readMaxKey();
        writeMaxKey(attribute);
        break;
      default:
        throw new IllegalArgumentException("unhandled BSON type: " + reader.getCurrentBsonType());
    }
  }

  private void pipeDocument(String attribute, BsonDocument value) {
    writeStartDocument(attribute);
    for (Map.Entry<String, BsonValue> cur : value.entrySet()) {
      pipeValue(cur.getKey(), cur.getValue());
    }
    writeEndDocument();
  }

  private void pipeArray(String attribute, BsonReader reader) {
    reader.readStartArray();
    writeStartArray(attribute);
    int i = 0;
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      pipeValue(Integer.toString(i), reader);
      ++i;
    }
    reader.readEndArray();
    writeEndArray();
  }

  private void pipeArray(String attribute, final BsonArray array) {
    writeStartArray(attribute);
    int i = 0;
    for (BsonValue cur : array) {
      pipeValue(Integer.toString(i), cur);
      ++i;
    }
    writeEndArray();
  }

  private void pipeJavascriptWithScope(String attribute, final BsonJavaScriptWithScope javaScriptWithScope) {
    writeJavaScriptWithScope(javaScriptWithScope.getCode());
    pipeDocument(attribute, javaScriptWithScope.getScope());
  }

  private void pipeValue(String attribute, final BsonValue value) {
    switch (value.getBsonType()) {
      case DOCUMENT:
        pipeDocument(attribute, value.asDocument());
        break;
      case ARRAY:
        pipeArray(attribute, value.asArray());
        break;
      case DOUBLE:
        writeDouble(attribute, value.asDouble().getValue());
        break;
      case STRING:
        writeString(attribute, value.asString().getValue());
        break;
      case BINARY:
        writeBinaryData(attribute, value.asBinary());
        break;
      case UNDEFINED:
        writeUndefined();
        break;
      case OBJECT_ID:
        writeObjectId(attribute, value.asObjectId().getValue());
        break;
      case BOOLEAN:
        writeBoolean(attribute, value.asBoolean().getValue());
        break;
      case DATE_TIME:
        writeDateTime(attribute, value.asDateTime().getValue());
        break;
      case NULL:
        writeNull();
        break;
      case REGULAR_EXPRESSION:
        writeRegularExpression(attribute, value.asRegularExpression());
        break;
      case JAVASCRIPT:
        writeJavaScript(attribute, value.asJavaScript().getCode());
        break;
      case SYMBOL:
        writeSymbol(attribute, value.asSymbol().getSymbol());
        break;
      case JAVASCRIPT_WITH_SCOPE:
        pipeJavascriptWithScope(attribute, value.asJavaScriptWithScope());
        break;
      case INT32:
        writeInt32(attribute, value.asInt32().getValue());
        break;
      case TIMESTAMP:
        writeTimestamp(attribute, value.asTimestamp());
        break;
      case INT64:
        writeInt64(attribute, value.asInt64().getValue());
        break;
      case DECIMAL128:
        writeDecimal128(attribute, value.asDecimal128().getValue());
        break;
      case MIN_KEY:
        writeMinKey(attribute);
        break;
      case DB_POINTER:
        writeDBPointer(attribute, value.asDBPointer());
        break;
      case MAX_KEY:
        writeMaxKey(attribute);
        break;
      default:
        throw new IllegalArgumentException("unhandled BSON type: " + value.getBsonType());
    }
  }
}
