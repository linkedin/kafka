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

import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;

/** Run against each real client archive before starting the migration. */
public final class LiBridgeMetadataChurnRetryTest {
    private LiBridgeMetadataChurnRetryTest() { }

    public static void main(String[] args) {
        List<String> failures = new ArrayList<>();
        for (Errors error : new Errors[] {Errors.STALE_CONTROLLER_EPOCH, Errors.NOT_CONTROLLER,
                Errors.NETWORK_EXCEPTION, Errors.REQUEST_TIMED_OUT, Errors.UNKNOWN_TOPIC_OR_PARTITION}) {
            check(failures, error.exception(), true, true);
        }
        for (Errors error : new Errors[] {Errors.TOPIC_ALREADY_EXISTS, Errors.INVALID_REPLICA_ASSIGNMENT}) {
            check(failures, error.exception(), true, false);
        }
        for (Errors error : new Errors[] {Errors.TOPIC_AUTHORIZATION_FAILED, Errors.CLUSTER_AUTHORIZATION_FAILED,
                Errors.INVALID_REQUEST, Errors.INVALID_CONFIG, Errors.KAFKA_STORAGE_ERROR,
                Errors.CORRUPT_MESSAGE, Errors.MESSAGE_TOO_LARGE}) {
            check(failures, error.exception(), false, false);
        }
        check(failures, new AssertionError("record mismatch"), false, false);
        check(failures, new InterruptedException("interrupted"), false, false);
        check(failures, new IllegalStateException("unexpected failure"), false, false);
        if (!failures.isEmpty()) {
            throw new AssertionError(failures.toString());
        }
        System.out.println("Metadata churn retry classification: passed");
    }

    private static void check(List<String> failures, Throwable cause, boolean mutation, boolean deletion) {
        if (LiBridgeMetadataChurn.retryMutation(cause) != mutation) {
            failures.add(cause.getClass().getSimpleName() + " mutation retry must be " + mutation);
        }
        if (LiBridgeMetadataChurn.retryDeletion(cause) != deletion) {
            failures.add(cause.getClass().getSimpleName() + " deletion retry must be " + deletion);
        }
    }
}
