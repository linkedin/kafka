/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.metrics.Metrics;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PoisonPillProcessTest {
    @Test
    public void testDieAttemptsHeapDumpAndHaltsProcess() throws Exception {
        Path dumpDirectory = Files.createTempDirectory("poison-pill-test");
        Path output = dumpDirectory.resolve("process-output.log");
        String javaBinary = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath();
        Process process = new ProcessBuilder(
            javaBinary,
            "-Xmx64m",
            "-cp",
            System.getProperty("java.class.path"),
            HaltProcess.class.getName(),
            dumpDirectory.toString()
        ).redirectErrorStream(true).redirectOutput(output.toFile()).start();

        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "PoisonPill child process did not halt");
            assertEquals(23, process.exitValue());
            String processOutput = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            assertTrue(processOutput.contains("PoisonPill dumping heap to"), processOutput);
            assertTrue(Files.size(dumpDirectory.resolve("dump.complete.hprof")) > 0);
            assertFalse(Files.exists(dumpDirectory.resolve("dump.inprogress.hprof")));
        } finally {
            process.destroyForcibly();
            Utils.delete(dumpDirectory.toFile());
        }
    }

    public static final class HaltProcess {
        public static void main(String[] args) {
            new PoisonPill(new Metrics()).die(new File(args[0]), 30_000, 23);
        }
    }
}
