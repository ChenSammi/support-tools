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

import org.preta.tools.ozone.benchmark.IoStats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "delete",
    description = "Benchmark OzoneManager Delete.",
    mixinStandardHelpOptions = true)
public class OmDeleteBenchmark extends AbstractOmBenchmark
{

  @Option(names = {"-d", "--numDeleteThreads"},
      description = "Number of delete threads.")
  private int deleteThreads;

  @Option(names = {"-v", "--volume"},
      description = "Ozone Volume.")
  private String volume;

  @Option(names = {"-b", "--bucket"},
      description = "Ozone Bucket.")
  private String bucket;

  @Option(names = {"-f", "--deleteObjectFile"},
      description = "The file contains the key to delete")
  private String fileName;

  @Option(names = {"-l", "--numKeyLimit"},
      description = "Max number of key to delete.")
  private long keyLimit;


  private final AtomicLong deleteKeyNamePointer;
  private BufferedReader reader;

  public OmDeleteBenchmark() {
    this.deleteThreads = 1;
    this.fileName = "";
    this.deleteKeyNamePointer = new AtomicLong(0);
    this.keyLimit = 0;
    this.volume = "";
    this.bucket = "";
  }

  public void execute() {

    System.out.println("Benchmarking OzoneManager Delete.");

    ExecutorService deleteExecutor = Executors.newFixedThreadPool(deleteThreads);
    File file = new File(fileName);

    try {
      reader = new BufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      System.out.println("Get file failure :" + e.getLocalizedMessage());
    }

    try {

      for (int j = 0; j < this.deleteThreads; j++) {
        deleteExecutor.submit(() -> {
          while (deleteKeyNamePointer.get() < keyLimit) {
            try {
              deleteKey(volume, bucket, getKeyNameToDelete());
            } catch (IOException e) {
              System.out.println("Failed to get key to delete :" + e.getLocalizedMessage());
              break;
            }
          }
        });
      }

      ScheduledExecutorService statsThread = Executors.newSingleThreadScheduledExecutor();
      statsThread.scheduleAtFixedRate(this::printStats, 5, 5, TimeUnit.MINUTES);

      deleteExecutor.shutdown();
      deleteExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      statsThread.shutdown();
      statsThread.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private String getKeyNameToDelete() throws IOException {
    return reader.readLine();
  }

  public void printStats() {
    final DecimalFormat df = new DecimalFormat("#.0000");
    final IoStats stats = getIoStats();
    System.out.println("================================================================");
    System.out.println(LocalDateTime.now());
    System.out.println("================================================================");
    System.out.println("Time elapsed: " + (stats.getElapsedTime() / 1000000000) + " sec.");
    System.out.println("Number of Threads: " + deleteThreads);
    System.out.println("Number of Keys deleted: " + stats.getKeysDeleted());
    System.out.println("Average Key delete time (CPU Time): " + df.format(stats.getAverageKeyDeleteCpuTime() / 1000000) + " milliseconds. ");
    System.out.println("Average Key delete time (Real): " + df.format((stats.getAverageKeyDeleteCpuTime() / deleteThreads) / 1000000 ) + " milliseconds. ");
    System.out.println("Max Key delete time (CPU Time): " + stats.getMaxKeyDeleteTime() / 1000000 + " milliseconds.");
    System.out.println("****************************************************************");
  }

}
