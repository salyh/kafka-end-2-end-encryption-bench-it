## Result
* JDK 1.8.0_45, VM 25.45-b02
* Mac OS 10.10.5
* MacBookPro11,2
* Processor: Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz


    Benchmark                                      Mode  Cnt       Score       Error  Units
    SerDeBenchmark.testDeserializeDecryption1k    thrpt    5  989378,065 ± 72106,945  ops/s -> 966 mb/s
    SerDeBenchmark.testDeserializeDecryption256k  thrpt    5    5809,043 ±   356,283  ops/s -> 1452 mb/s
    SerDeBenchmark.testDeserializeDecryption4k    thrpt    5  385975,071 ± 25585,912  ops/s -> 1500 mb/s
    SerDeBenchmark.testDeserializeDecryption8k    thrpt    5  197596,274 ± 27436,372  ops/s -> 1543 mb/s
    
    SerDeBenchmark.testSerializeEncryption1k      thrpt    5  179025,718 ± 76085,422  ops/s -> 174 mb/s
    SerDeBenchmark.testSerializeEncryption256k    thrpt    5    3683,626 ±   496,401  ops/s -> 920 mb/s
    SerDeBenchmark.testSerializeEncryption4k      thrpt    5  133651,065 ± 65639,083  ops/s -> 522 mb/s
    SerDeBenchmark.testSerializeEncryption8k      thrpt    5   98810,508 ± 19159,625  ops/s -> 771 mb/s
