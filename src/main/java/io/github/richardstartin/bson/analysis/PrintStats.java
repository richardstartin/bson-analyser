package io.github.richardstartin.bson.analysis;

import org.bson.BsonBinaryReader;
import org.bson.RawBsonDocument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class PrintStats {


  public static void main(String... args) throws IOException {
    Map<String, Integer> sizes = new HashMap<>();
    analyse("raw.json", sizes);
    analyse("raw-minified.json", sizes);
    analyse("normalised.json", sizes);
    analyse("indexed.json", sizes);
    analyse("indexed-bitset.json", sizes);
    analyse("matrix.json", sizes);
    analyse("binary.json", sizes);
    analyse("minimised.json", sizes);
    System.out.println(sizes);
  }


  public static void analyse(String filename, Map<String, Integer> sizes) throws IOException {
    RawBsonDocument doc = RawBsonDocument.parse(getJson(filename));
    sizes.put(filename, doc.getByteBuffer().asNIO().limit());
    BsonOverheadAnalyser analyser = new BsonOverheadAnalyser();
    analyser.pipe(new BsonBinaryReader(doc.getByteBuffer().asNIO()));
    System.out.println(filename);
    analyser.printStatistics(System.out);
    System.out.println();
    System.out.println();
  }



  private static String getJson(String filename) throws IOException {
    return new String(Files.readAllBytes(Path.of(System.getProperty("user.dir"), "src/json", filename)));
  }





}
