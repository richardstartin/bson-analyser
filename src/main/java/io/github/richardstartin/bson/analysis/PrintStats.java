package io.github.richardstartin.bson.analysis;

import org.bson.BsonBinaryReader;
import org.bson.RawBsonDocument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PrintStats {


  public static void main(String... args) throws IOException {
    analyse("raw.json");
    analyse("raw-minified.json");
    analyse("normalised.json");
    analyse("indexed.json");
    analyse("indexed-bitset.json");
    analyse("matrix.json");
    analyse("binary.json");
    analyse("minimised.json");
//    analyse("metrics2-minified.json");
//    analyse("metrics3.json");
//    analyse("metrics3-minified.json");
//    analyse("metrics4.json");
//    analyse("metrics4-minified.json");
//    analyse("metrics5.json");
//    analyse("metrics5-minified.json");
//    analyse("metrics6.json");
//    analyse("metrics6-minified.json");
//    analyse("metrics7.json");
//    analyse("metrics7-minified.json");
//    analyse("metrics8.json");
  }


  public static void analyse(String filename) throws IOException {
    RawBsonDocument doc = RawBsonDocument.parse(getJson(filename));
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
