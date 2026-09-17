"""Independent manifest, CRC32C and shared-enum conformance fixtures."""

import json
import unittest
from pathlib import Path

from bqip.capture.format import crc32c
from bqip.contracts import operational, values
from bqip.manifests import canonical_bytes, canonical_manifest, manifest_hash, parse_manifest

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


class ConformanceTests(unittest.TestCase):
    def test_independent_jcs_expected_bytes(self):
        for vector in fixture("manifests/conformance.json")["valid"]:
            with self.subTest(name=vector["name"]):
                document = parse_manifest(vector["input_json"])
                self.assertEqual(canonical_bytes(document).hex(), vector["jcs_utf8_hex"])
                self.assertEqual(canonical_manifest(document).hex(), vector["manifest_utf8_hex"])
                self.assertEqual(manifest_hash(document).hex(), vector["manifest_sha256"])

    def test_jcs_rejects_outside_accepted_domain(self):
        for vector in fixture("manifests/conformance.json")["invalid"]:
            with self.subTest(name=vector["name"]), self.assertRaises(ValueError):
                parse_manifest(vector["input_json"])

    def test_crc32c_standard_and_every_golden_record(self):
        for vector in fixture("capture/crc32c.json"):
            self.assertEqual(f"{crc32c(bytes.fromhex(vector['input_hex'])):08x}", vector["crc32c_hex"])
        for vector in fixture("capture/frames.json"):
            body = bytes.fromhex(vector["metadata_hex"] + vector["payload_hex"])
            expected = int.from_bytes(bytes.fromhex(vector["frame_hex"])[-4:], "big")
            self.assertEqual(crc32c(body), expected)

    def test_shared_enum_external_expected_values(self):
        classes = {"ReplayMode": values.ReplayMode, "PayloadKind": values.PayloadKind,
                   "RawSegmentState": values.RawSegmentState,
                   "OperationalKind": operational.OperationalKind, "CauseCode": operational.CauseCode,
                   "RawLossStatus": operational.RawLossStatus,
                   "RealtimeContinuity": operational.RealtimeContinuity}
        for name, vectors in fixture("contracts/enums.json").items():
            enum = classes[name]
            self.assertEqual([(item.name, item.value) for item in enum],
                             [(item["token"], item["number"]) for item in vectors])
            with self.assertRaises(ValueError):
                enum(999)


if __name__ == "__main__":
    unittest.main()
