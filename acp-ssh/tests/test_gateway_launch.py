# SPDX-License-Identifier: Apache-2.0
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

GATEWAY = Path(__file__).resolve().parents[1] / "gateway"
sys.path.insert(0, str(GATEWAY))
spec = importlib.util.spec_from_file_location(
    "independent_gateway_launch", GATEWAY / "launch.py"
)
launch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launch)


class ConfigurationTests(unittest.TestCase):
    def test_environment_only_and_no_implicit_auth_store(self):
        with tempfile.TemporaryDirectory() as directory:
            base = {"HOME": directory, "AGY_NATIVE_BINARY": sys.executable}
            binary, env = launch.configuration(base)
            self.assertEqual(binary, Path(sys.executable).absolute())
            self.assertNotIn("AGY_LLM_GATEWAY_API_KEY", env)
            _, env = launch.configuration(
                {
                    **base,
                    "AGY_LLM_GATEWAY_URL": "http://example.invalid",
                    "AGY_LLM_GATEWAY_API_KEY": "synthetic",
                }
            )
            self.assertEqual(env["AGY_LLM_GATEWAY_API_KEY"], "synthetic")

    def test_config_env_key_reference_and_precedence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gateway.json"
            path.write_text(
                json.dumps(
                    {
                        "native_binary": sys.executable,
                        "gateway_url": "http://config.invalid",
                        "api_key_env": "MY_TEST_KEY",
                    }
                )
            )
            base = {
                "HOME": directory,
                "AGY_GATEWAY_CONFIG": str(path),
                "MY_TEST_KEY": "synthetic",
            }
            _, env = launch.configuration(base)
            self.assertEqual(env["AGY_LLM_GATEWAY_API_KEY"], "synthetic")
            _, env = launch.configuration(
                {
                    **base,
                    "AGY_LLM_GATEWAY_URL": "http://env.invalid",
                    "AGY_LLM_GATEWAY_API_KEY": "override",
                }
            )
            self.assertEqual(env["AGY_LLM_GATEWAY_URL"], "http://env.invalid")
            self.assertEqual(env["AGY_LLM_GATEWAY_API_KEY"], "override")

    def test_explicit_missing_config_and_invalid_key_reference_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gateway.json"
            env = {
                "HOME": directory,
                "AGY_NATIVE_BINARY": sys.executable,
                "AGY_GATEWAY_CONFIG": str(path),
            }
            with self.assertRaises(ValueError):
                launch.configuration(env)
            path.write_text(json.dumps({"api_key_env": "invalid key name"}))
            with self.assertRaises(ValueError):
                launch.configuration(env)


if __name__ == "__main__":
    unittest.main()
