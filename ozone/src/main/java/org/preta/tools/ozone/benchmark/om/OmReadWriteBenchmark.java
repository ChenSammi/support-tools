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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.preta.tools.ozone.ReadableTimestampConverter;

import org.preta.tools.ozone.benchmark.IoStats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "read-write",
    description = "Benchmark OzoneManager Read/Write.",
    mixinStandardHelpOptions = true)
public class OmReadWriteBenchmark extends AbstractOmBenchmark
{

  @Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  private long runtime;

  @Option(names = {"-u", "--user"},
      description = "User Name.")
  private String user;

  @Option(names = {"-rv", "--readVolume"},
      description = "Ozone Volume for read.")
  private String readVolume;

  @Option(names = {"-wv", "--writeVolume"},
      description = "Ozone Volume for write.")
  private String writeVolume;

  @Option(names = {"-rb", "--readBucket"},
      description = "Ozone Bucket for read.")
  private String readBucket;

  @Option(names = {"-wb", "--writeBucket"},
      description = "Ozone Bucket for write.")
  private String writeBucket;

  @Option(names = {"-wp", "--writeKeyPrefix"},
      description = "Write Key Prefix.")
  private String writeKeyNamePrefix;

  @Option(names = {"-rp", "--readKeyPrefix"},
      description = "Read Key Prefix.")
  private String readKeyNamePrefix;

  @Option(names = {"-w", "--numWriteThreads"},
      description = "Number of writer threads.")
  private int writerThreads;

  @Option(names = {"-r", "--numReaderThreads"},
      description = "Number of reader threads.")
  private int readerThreads;

  @Option(names = {"-wl", "--numWriteKeyLimit"},
      description = "Max number of key to write.")
  private long writeKeyLimit;

  @Option(names = {"-ws", "--writeStartIndex"},
      description = "Key Suffix Start Index to write.")
  private long writeStartIndex;

  @Option(names = {"-rl", "--numReadKeyLimit"},
      description = "Max number of key to read.")
  private long readKeyLimit;

  private final AtomicLong writeKeyNamePointer;
  private final AtomicLong readKeyNamePointer;

  public OmReadWriteBenchmark() {
    this.user = "admin";
    this.writeVolume = "instagram";
    this.writeBucket = "images";
    this.writeKeyNamePrefix = "";
    this.readKeyNamePrefix = "";
    this.writerThreads = 10;
    this.readerThreads = 10;
    this.writeKeyNamePointer = new AtomicLong(0);
    this.readKeyNamePointer = new AtomicLong(0);
    this.readKeyLimit = Long.MAX_VALUE;
    this.writeKeyLimit = Long.MAX_VALUE;
    this.writeStartIndex = 0;
  }

  public void execute() {
    try {
      if (StringUtils.isEmpty(writeKeyNamePrefix)) {
        writeKeyNamePrefix += UUID.randomUUID().toString();
      }
      if (Strings.isEmpty(readKeyNamePrefix)) {
        System.out.println("readKeyNamePrefix cannot be empty in Read/Write test");
        System.exit(-1);
      }
      if (Strings.isEmpty(readVolume)) {
        readVolume = "instagram";
        System.out.println("Set read volume to " + readVolume);
      }
      if (Strings.isEmpty(readBucket)) {
        readBucket = "images";
        System.out.println("Set read bucket to " + readBucket);
      }
      if (writeStartIndex > 0) {
        this.writeKeyNamePointer.set(writeStartIndex);
        if (writeKeyLimit != Long.MAX_VALUE) {
          writeKeyLimit += writeStartIndex;
        }
      }
      System.out.println("Benchmarking OzoneManager Read/Write.");
      System.out.println("Start read from key " + readKeyNamePrefix + "-" +
          readKeyNamePointer.get() + ", count " + readKeyLimit);
      System.out.println("Start write from key " + writeKeyNamePrefix + "-" +
          writeKeyNamePointer.get() + ", count " + writeKeyLimit);
      final long endTimeInNs = System.nanoTime() + (runtime * 1000000000L);
      createVolume(user, writeVolume);
      createBucket(writeVolume, writeBucket);

      ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThreads);
      ExecutorService readExecutor = Executors.newFixedThreadPool(readerThreads);

      for (int i = 0; i < this.writerThreads; i++) {
        writeExecutor.submit(() -> {
          while (System.nanoTime() < endTimeInNs && writeKeyNamePointer.get() < writeKeyLimit) {
            writeKey(writeVolume, writeBucket, getKeyNameToWrite());
          }
        });
      }

      for (int i = 0; i < this.readerThreads; i++) {
        readExecutor.submit(() -> {
          while (System.nanoTime() < endTimeInNs && readKeyNamePointer.get() < readKeyLimit) {
            readKey(readVolume, readBucket, getKeyNameToRead());
          }
        });
      }

      ScheduledExecutorService statsThread = Executors.newSingleThreadScheduledExecutor();
      statsThread.scheduleAtFixedRate(this::printStats, 5, 5, TimeUnit.MINUTES);

      writeExecutor.shutdown();
      readExecutor.shutdown();
      writeExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      readExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);

      statsThread.shutdown();
      statsThread.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private String getKeyNameToWrite() {
    return writeKeyNamePrefix + "-" + writeKeyNamePointer.incrementAndGet();
  }

  private String getKeyNameToRead() {
    return readKeyNamePrefix + "-" + readKeyNamePointer.incrementAndGet();
  }

  public void printStats() {
    final DecimalFormat df = new DecimalFormat("#.0000");
    final IoStats stats = getIoStats();
    System.out.println("================================================================");
    System.out.println(LocalDateTime.now());
    System.out.println("================================================================");
    System.out.println("Time elapsed: " + (stats.getElapsedTime() / 1000000000) + " sec.");
    System.out.println("Number of Read Threads: " + readerThreads);
    System.out.println("Number of Write Threads: " + writerThreads);
    System.out.println("Number of Keys read: " + stats.getKeysRead());
    System.out.println("Number of Keys written: " + stats.getKeysCreated());
    System.out.println("Average Key read time (CPU Time): " + df.format(stats.getAverageKeyReadCpuTime() / 1000000) + " milliseconds. ");
    System.out.println("Average Key read time (Real): " + df.format((stats.getAverageKeyReadCpuTime() / readerThreads) / 1000000 ) + " milliseconds. ");
    System.out.println("Max Key read time (CPU Time): " + stats.getMaxKeyReadTime() / 1000000 + " milliseconds.");
    System.out.println("Average Key write time (CPU Time): " + df.format(stats.getAverageKeyWriteCpuTime() / 1000000) + " milliseconds. ");
    System.out.println("Average Key write time (Real): " + df.format((stats.getAverageKeyWriteCpuTime() / writerThreads) / 1000000 ) + " milliseconds. ");
    System.out.println("Max Key write time (CPU Time): " + stats.getMaxKeyWriteTime() / 1000000 + " milliseconds.");
    System.out.println("****************************************************************");
  }
}
