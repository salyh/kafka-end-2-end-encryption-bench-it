package de.saly.kafka.crypto.benchmark;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@State(Scope.Benchmark)
public class Main {

    public static void main(String[] args) throws RunnerException {
        
        String filename = "kafkae2ee_bench_result";
        
        System.out.println("Benchmark is running - Output will be written to "+filename+".(json|txt)");
        
        final Options opt = new OptionsBuilder()
                .include(".*Benchmark*")
                .forks(1)
                .warmupIterations(5)
                .measurementIterations(5)
                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)
                .verbosity(VerboseMode.EXTRA)
                //.jvmArgs("-Xmx" + MEMORY, "-Dfile.encoding=utf-8", "-Dbenchmark.impl="+parserClasss)
                .resultFormat(ResultFormatType.JSON)
                .result(filename+".json")
                .output(filename+".txt")
                .build();
                new Runner(opt).run();
                
        System.out.println("Benchmark finished");
    }
}
