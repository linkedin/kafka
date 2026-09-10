# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run verification commands with deadlines and retained output."""

import contextlib
import csv
import os
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


class Commands:
    def __init__(self, evidence, environment, resume=False, dry_run=False):
        self.evidence = Path(evidence)
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.environment = dict(environment)
        self.resume = resume
        self.dry_run = dry_run
        self.timeout = int(environment.get("BRIDGE_VERIFY_COMMAND_TIMEOUT_SECONDS", "3600"))
        if self.timeout <= 0:
            raise ValueError("BRIDGE_VERIFY_COMMAND_TIMEOUT_SECONDS must be positive")
        self.records = self.evidence / "commands.tsv"
        self.results = {}
        if resume and self.records.is_file():
            with self.records.open(newline="", encoding="utf-8") as source:
                for row in csv.DictReader(source, delimiter="\t"):
                    self.results[row["command"]] = row["result"]
        else:
            self.records.write_text("command\tduration_seconds\tresult\n", encoding="utf-8")

    def record(self, label, started, result):
        with self.records.open("a", encoding="utf-8", newline="") as output:
            csv.writer(output, delimiter="\t").writerow((label, round(time.monotonic() - started, 3), result))
        self.results[label] = result

    def run(self, label, args, cwd, extra_env=None, always=False):
        print(f"===== {label} =====", flush=True)
        if self.resume and not always and self.results.get(label) == "passed":
            print(f"Reusing previously passed command {label}", flush=True)
            return
        environment = dict(self.environment, **(extra_env or {}))
        command = list(map(str, args))
        if self.dry_run:
            exports = " ".join(f"{key}={shlex.quote(str(value))}" for key, value in (extra_env or {}).items())
            print(f"DRY RUN: {exports} {shlex.join(command)}", flush=True)
            return
        log_path = self.evidence / f"{label}.log"
        if log_path.exists():
            attempt = 1
            while (self.evidence / f"{label}-attempt-{attempt}.log").exists():
                attempt += 1
            shutil.copyfile(log_path, self.evidence / f"{label}-attempt-{attempt}.log")
        started = time.monotonic()
        result = "failed(start)"
        with log_path.open("w", encoding="utf-8") as log:
            log.write(f"Command: {shlex.join(command)}\nDirectory: {cwd}\n")
            try:
                process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, text=True, errors="replace", start_new_session=True)
            except OSError:
                self.record(label, started, result)
                raise
            reader_errors = []

            def copy_output():
                try:
                    for line in process.stdout:
                        log.write(line)
                        log.flush()
                        sys.stdout.write(line)
                        sys.stdout.flush()
                except Exception as error:
                    reader_errors.append(error)

            reader = threading.Thread(target=copy_output, daemon=True)
            reader.start()
            try:
                status = process.wait(timeout=self.timeout)
                reader.join(timeout=10)
                if reader.is_alive() or reader_errors:
                    raise RuntimeError(f"Could not retain complete output for {label}")
                result = "passed" if status == 0 else f"failed({status})"
                if status != 0:
                    raise subprocess.CalledProcessError(status, command)
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                reader.join(timeout=10)
                raise
            finally:
                process.stdout.close()
                self.record(label, started, result)
