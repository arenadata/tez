/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestTezThreadDumpHelper {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCollectorWritesUtf8ThreadDump() throws Exception {
    String name = "thread-dump-\u041f\u0440\u043e\u0431\u0430";
    Path directory = new Path(temporaryFolder.getRoot().toURI());
    try (FileSystem fs = FileSystem.newInstanceLocal(new Configuration(false))) {
      Class<?> collectorClass =
          Class.forName("org.apache.tez.runtime.TezThreadDumpHelper$ThreadDumpCollector");
      Constructor<?> constructor =
          collectorClass.getDeclaredConstructor(Path.class, String.class, FileSystem.class);
      constructor.setAccessible(true);
      Runnable collector = (Runnable) constructor.newInstance(directory, name, fs);

      collector.run();

      FileStatus[] dumps = fs.listStatus(directory, path -> path.getName().endsWith(".jstack"));
      assertEquals(1, dumps.length);
      try (FSDataInputStream input = fs.open(dumps[0].getPath());
          ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
        IOUtils.copyBytes(input, bytes, 4096, false);
        String content = new String(bytes.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(content.startsWith("Process Thread Dump: " + name + System.lineSeparator()));
        assertTrue(content.contains("active threads"));
      }
    }
  }
}
