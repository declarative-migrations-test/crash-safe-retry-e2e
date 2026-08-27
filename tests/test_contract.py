import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = json.loads((ROOT / "source.json").read_text(encoding="utf-8"))
SCENARIO = json.loads((ROOT / "scenario.json").read_text(encoding="utf-8"))

REQUIRED = {
    "crash-before-txn-is-retryable",
    "crash-during-ddl-rolls-back",
    "crash-after-ddl-before-history-recovers-without-double-apply",
    "crash-after-history-before-ack-is-idempotent",
    "stolen-lock-refuses-writes",
    "stale-lock-refuses-writes",
    "checksum-mismatch-refuses-writes",
}


class CanonicalMigrationProfileContract(unittest.TestCase):
    def test_identity_and_schema(self):
        self.assertEqual(SOURCE["schemaVersion"], 1)
        self.assertEqual(SCENARIO["schemaVersion"], 1)
        self.assertEqual(
            SCENARIO["repository"],
            "declarative-migrations-test/crash-safe-retry-e2e",
        )

    def test_immutable_source_pins(self):
        self.assertGreaterEqual(len(SOURCE["productionSources"]), 2)
        for source in SOURCE["productionSources"]:
            self.assertRegex(source["commit"], r"^[0-9a-f]{40}$")

    def test_unique_actionable_invariants(self):
        invariants = SCENARIO["requiredInvariants"]
        self.assertEqual(set(invariants), REQUIRED)
        self.assertEqual(len(invariants), len(REQUIRED))

    def test_fail_closed_credential_free_policy(self):
        policy = SCENARIO["policy"]
        self.assertIs(policy["failClosed"], True)
        self.assertIs(policy["credentialsInPullRequests"], False)
        self.assertIs(policy["immutableProductionPins"], True)
        self.assertIs(policy["destructiveFixtures"], False)
        self.assertIs(policy["liveEnvironmentRequiredForCertification"], False)

    def test_no_embedded_credentials_or_false_certification(self):
        raw = (ROOT / "source.json").read_text().lower() + (
            ROOT / "scenario.json"
        ).read_text().lower()
        for marker in (
            "ghp_",
            "github_pat_",
            "authorization: bearer ",
            "client_secret=",
            "password=",
        ):
            self.assertNotIn(marker, raw)
        self.assertNotIn('"livecertified":true', raw.replace(" ", ""))


if __name__ == "__main__":
    unittest.main()
