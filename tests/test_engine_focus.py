import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCENARIO = json.loads((ROOT / "scenario.json").read_text())
EXPECTED_REPOSITORY = "declarative-migrations-test/crash-safe-retry-e2e"


class EngineFocusE2E(unittest.TestCase):
    def test_repository_engine_and_focus_identity(self):
        self.assertEqual(SCENARIO["schemaVersion"], 1)
        self.assertEqual(SCENARIO["repository"], EXPECTED_REPOSITORY)
        self.assertEqual(SCENARIO["engine"], "sqlite")
        self.assertEqual(SCENARIO["integrationMode"], "local-file")
        self.assertIn("crash-safe retry", SCENARIO["focus"])

    def test_required_invariants_are_unique_executable_contract_names(self):
        invariants = SCENARIO["requiredInvariants"]
        self.assertEqual(len(invariants), len(set(invariants)))
        for invariant in invariants:
            self.assertRegex(invariant, r"^[a-z0-9]+(?:-[a-z0-9]+)+$")
        self.assertEqual(
            set(invariants),
            {
                "crash-before-txn-is-retryable",
                "crash-during-ddl-rolls-back",
                "crash-after-ddl-before-history-recovers-without-double-apply",
                "crash-after-history-before-ack-is-idempotent",
                "stolen-lock-refuses-writes",
                "stale-lock-refuses-writes",
                "checksum-mismatch-refuses-writes",
            },
        )

    def test_policy_fails_closed_without_external_credentials(self):
        policy = SCENARIO["policy"]
        self.assertIs(policy["failClosed"], True)
        self.assertIs(policy["credentialsInPullRequests"], False)
        self.assertIs(policy["immutableProductionPins"], True)
        self.assertIs(policy["destructiveFixtures"], False)
        self.assertIs(policy["liveEnvironmentRequiredForCertification"], False)


if __name__ == "__main__":
    unittest.main()
