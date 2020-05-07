/*
 * Copyright 2019 Nanda kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.preta.tools.ozone.benchmark.om;

import org.preta.tools.ozone.ReadableTimestampConverter;
import org.preta.tools.ozone.benchmark.IoStats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "read",
    description = "Benchmark OzoneManager Read.",
    mixinStandardHelpOptions = true)
public class OmReadBenchmark  extends AbstractOmBenchmark {

  @Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  private long runtime;

  @Option(names = {"-v", "--volume"},
      description = "Ozone Volume.")
  private String volume;

  @Option(names = {"-b", "--bucket"},
      description = "Ozone Bucket.")
  private String bucket;

  @Option(names = {"-p", "--keyPrefix"},
      description = "Key Prefix.")
  private String keyNamePrefix;

  @Option(names = {"-r", "--numReaderThreads"},
      description = "Number of reader threads.")
  private int readerThreads;

  @Option(names = {"-l", "--numKeyLimit"},
      description = "Max number of key to read.")
  private long keyLimit;

  private final AtomicLong readKeyNamePointer;

  public OmReadBenchmark() {
    this.volume = "instagram";
    this.bucket = "images";
    this.keyNamePrefix = "";
    this.readerThreads = 10;
    this.readKeyNamePointer = new AtomicLong(0);
    this.keyLimit = Long.MAX_VALUE;
  }

  public void execute() {
    try {
      System.out.println("Benchmarking OzoneManager Read.");

      final long endTimeInNs = System.nanoTime() + runtime * 1000000000L;
      final ExecutorService executor = Executors.newFixedThreadPool(readerThreads);
      for (int i = 0; i < readerThreads; i++) {
        executor.submit(() -> {
          while (System.nanoTime() < endTimeInNs && readKeyNamePointer.get() < keyLimit) {
            readKey(volume, bucket, getKeyNameToRead());
          }
        });
      }

      ScheduledExecutorService statsThread = Executors.newSingleThreadScheduledExecutor();
      statsThread.scheduleAtFixedRate(this::printStats, 5, 5, TimeUnit.MINUTES);

      executor.shutdown();
      executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      statsThread.shutdown();
      statsThread.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);

    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private String getKeyNameToRead() {
    return keyNamePrefix + "-" + readKeyNamePointer.incrementAndGet();
  }



  public void printStats() {
    final DecimalFormat df = new DecimalFormat("#.0000");
    final IoStats stats = getIoStats();
    System.out.println("================================================================");
    System.out.println(LocalDateTime.now());
    System.out.println("================================================================");
    System.out.println("Time elapsed: " + (stats.getElapsedTime() / 1000000000) + " sec.");
    System.out.println("Number of Threads: " + readerThreads);
    System.out.println("Number of Keys read: " + stats.getKeysRead());
    System.out.println("Average Key read time (CPU Time): " + df.format(stats.getAverageKeyReadCpuTime() / 1000000) + " milliseconds. ");
    System.out.println("Average Key read time (Real): " + df.format((stats.getAverageKeyReadCpuTime() / readerThreads) / 1000000 ) + " milliseconds. ");
    System.out.println("Max Key read time (CPU Time): " + stats.getMaxKeyReadTime() / 1000000 + " milliseconds.");
    System.out.println("****************************************************************");
  }
}

