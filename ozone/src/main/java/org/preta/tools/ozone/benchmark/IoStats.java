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

package org.preta.tools.ozone.benchmark;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicLong;

public class IoStats {

  // All the time represented in this class are in nanoseconds.

  private final long startTime = System.nanoTime();

  // Write metrics.
  private final AtomicLong keysCreated = new AtomicLong(0);
  private final AtomicDouble keyWriteCpuTime = new AtomicDouble(0);
  private final AtomicLong maxKeyWriteTime = new AtomicLong(0);


  // Read metrics.
  private final AtomicLong keysRead = new AtomicLong(0);
  private final AtomicDouble keyReadCpuTime = new AtomicDouble(0);
  private final AtomicLong maxKeyReadTime = new AtomicLong(0);

  // Delete metrics.
  private final AtomicLong keysDeleted = new AtomicLong(0);
  private final AtomicDouble keyDeleteCpuTime = new AtomicDouble(0);
  private final AtomicLong maxKeyDeleteTime = new AtomicLong(0);

  public long getStartTime() {
    return startTime;
  }

  public long getElapsedTime() {
    return System.nanoTime() - startTime;
  }

  public void incrKeysCreated() {
    keysCreated.incrementAndGet();
  }

  public long getKeysCreated() {
    return keysCreated.get();
  }

  public long getKeysDeleted() {
    return keysDeleted.get();
  }

  public void addKeyWriteCpuTime(long writeTime) {
    keyWriteCpuTime.getAndAdd(writeTime);
  }

  public void addKeyReadCpuTime(long readTime) {
    keyReadCpuTime.getAndAdd(readTime);
  }

  public void addKeyDeleteCpuTime(long deleteTime) {
    keyDeleteCpuTime.getAndAdd(deleteTime);
  }

  public double getKeyWriteCpuTime() {
    return keyWriteCpuTime.get();
  }

  public double getAverageKeyWriteCpuTime() {
    return keyWriteCpuTime.get() / keysCreated.get();
  }

  public double getAverageKeyReadCpuTime() {
    return keyReadCpuTime.get() / keysRead.get();
  }

  public double getAverageKeyDeleteCpuTime() {
    return keyDeleteCpuTime.get() / keysDeleted.get();
  }

  public void setMaxKeyWriteTime(long keyWriteTime) {
    while(true) {
      final long oldTime = maxKeyWriteTime.get();
      if(oldTime >= keyWriteTime || maxKeyWriteTime.compareAndSet(oldTime, keyWriteTime)) {
        return;
      }
    }
  }

  public void setMaxKeyReadTime(long keyReadTime) {
    while(true) {
      final long oldTime = maxKeyReadTime.get();
      if(oldTime >= keyReadTime || maxKeyReadTime.compareAndSet(oldTime, keyReadTime)) {
        return;
      }
    }
  }

  public void setMaxKeyDeleteTime(long keyDeleteTime) {
    while(true) {
      final long oldTime = maxKeyDeleteTime.get();
      if(oldTime >= keyDeleteTime || maxKeyDeleteTime.compareAndSet(oldTime, keyDeleteTime)) {
        return;
      }
    }
  }

  public long getMaxKeyWriteTime() {
    return maxKeyWriteTime.get();
  }

  public long getMaxKeyReadTime() {
    return maxKeyReadTime.get();
  }

  public long getMaxKeyDeleteTime() {
    return maxKeyDeleteTime.get();
  }

  public void incrKeysRead() {
    keysRead.incrementAndGet();
  }

  public void incrKeysDeleted() {
    keysDeleted.incrementAndGet();
  }

  public long getKeysRead() {
    return keysRead.get();
  }

}
