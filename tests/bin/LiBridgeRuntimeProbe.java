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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jute.BinaryInputArchive;
import org.apache.zookeeper.ZooKeeper;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Run on the packaged broker/wrapper classpath, not the isolated vendor test classpath. */
public final class LiBridgeRuntimeProbe {
    private LiBridgeRuntimeProbe() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected <output.json> <require-pagination:true|false>");
        }
        if (!args[1].equals("true") && !args[1].equals("false")) {
            throw new IllegalArgumentException("require-pagination must be true or false");
        }
        boolean supported;
        try {
            supported = List.class.isAssignableFrom(ZooKeeper.class.getMethod(
                "getAllChildrenPaginated", String.class, boolean.class).getReturnType());
        } catch (NoSuchMethodException e) {
            supported = false;
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("contract_version", 2);
        evidence.put("collected_at_utc", Instant.now().toString());
        evidence.put("pagination_supported", supported);
        describe(evidence, "zookeeper", ZooKeeper.class);
        describe(evidence, "jute", BinaryInputArchive.class);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(args[0]).toFile(), evidence);
        if (Boolean.parseBoolean(args[1]) && !supported) {
            throw new IllegalStateException("Packaged runtime lacks LinkedIn ZooKeeper pagination; do not admit this broker");
        }
    }

    private static void describe(Map<String, Object> evidence, String name, Class<?> type) throws Exception {
        Path path = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[65536];
            int size;
            while ((size = input.read(buffer)) != -1) {
                digest.update(buffer, 0, size);
            }
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) {
            hex.append(String.format("%02x", value));
        }
        evidence.put(name + "_jar", path.toString());
        evidence.put(name + "_sha256", hex.toString());
        evidence.put(name + "_version", type.getPackage().getImplementationVersion());
    }
}
