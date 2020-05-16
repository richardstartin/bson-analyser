package io.github.richardstartin.bson.analysis;

import org.bson.Document;
import org.bson.types.Binary;

import java.nio.ByteBuffer;

public class Util {

  public static void main(String... args) {
    System.out.println(new Document("tag1", new Binary(createBitSet(0,1,2,3,4,5,6,7,8,9,10,11))).toJson());
    System.out.println(new Document("tag2", new Binary(createBitSet(0,1,2,3,8,9,10,11))).toJson());
    System.out.println(new Document("tag3", new Binary(createBitSet(0,1,2,3,4,5,6,7))).toJson());
    System.out.println(new Document("tag4", new Binary(createBitSet(4,5,6,7))).toJson());
    System.out.println(new Document("values", new Binary(createArray(    1029831.102938,
            1129831.102938,
            1129831.102938,
            1229831.102938,
            1029831.102938,
            1129831.102938,
            1129831.102938,
            1229831.102938,
            1029831.102938,
            1129831.102938,
            1129831.102938,
            1229831.102938))).toJson());

  }

  private static byte[] createArray(double... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * 8);
    for (double value : values) {
      buffer.putDouble(value);
    }
    return buffer.array();
  }


  private static byte[] createBitSet(int... data) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    long mask = 0L;
    for (int datum : data) {
      mask |= (1L << datum);
    }
    buffer.putLong(0, mask);
    byte[] result = new byte[(data[data.length - 1] >>> 3) + 1];
    buffer.get(result);
    return result;
  }
}
