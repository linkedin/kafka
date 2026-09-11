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

import re
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "bin"))
from li_bridge_contract import BRIDGE_GATES, FEATURES, PHASES, scenario_spec
from li_bridge_mixed_cluster_smoke import Migration, verify_connect_records
from li_bridge_preflight import parse_properties, zk_command


class LiBridgeEntryPointsTest(unittest.TestCase):
    def test_bridge_shell_entry_points_follow_the_small_wrapper_style(self):
        directory = Path(__file__).parents[1] / "bin"
        for name in ("li_bridge_mixed_cluster_smoke.sh", "verify_li_bridge.sh",
                     "stage_li_bridge_ivy.sh", "verify_li_bridge_release.sh"):
            with self.subTest(name=name):
                lines = (directory / name).read_text().splitlines()
                self.assertEqual("#!/bin/bash", lines[0])
                self.assertTrue(lines[2].startswith("# "))
                self.assertLess(len(lines), 100)
                self.assertTrue(all(len(line) <= 80 and "\t" not in line for line in lines))
                subprocess.run(["bash", "-n", str(directory / name)], check=True)

    def test_release_gate_cannot_succeed_with_missing_inputs(self):
        script = Path(__file__).parents[1] / "bin/verify_li_bridge_release.sh"
        with mock.patch.dict("os.environ", {}, clear=True):
            result = subprocess.run(["bash", str(script)], capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("published 3.0 archive", result.stderr)


if __name__ == "__main__":
    unittest.main()
