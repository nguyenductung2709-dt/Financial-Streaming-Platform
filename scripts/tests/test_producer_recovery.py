import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "verify-producer-recovery.py"
spec = importlib.util.spec_from_file_location("producer_recovery", SCRIPT)
verification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verification)


class RecoveryReferenceTests(unittest.TestCase):
    def test_fixtures_have_unique_rows_and_matching_reference_results(self):
        normal = verification.fixture()
        extended = verification.fixture(extended=True)
        self.assertEqual(len(normal), 361)
        self.assertEqual(len(extended), 757)
        self.assertEqual(len({r["row"] for r in extended}), len(extended))
        windows, advice = verification.reference(normal)
        self.assertEqual((windows, advice), verification.reference(extended))
        self.assertEqual(len(windows), 180)
        for symbol in verification.SYMBOLS:
            actions = [v["action"] for v in advice if v["symbol"] == symbol]
            self.assertEqual(actions, ["ADVISORY_ACTION_BUY", "ADVISORY_ACTION_SELL", "ADVISORY_ACTION_BUY"])

    def test_identical_output_duplicates_are_counted(self):
        window = verification.reference(verification.fixture())[0][0]
        result = verification.compare([window], [window, window])
        self.assertEqual(result["duplicates"], 1)
        self.assertEqual(result["conflictingDuplicates"], 0)
        self.assertEqual(result["mismatches"], 0)

    def test_conflicting_duplicate_is_not_hidden_by_a_later_correct_value(self):
        window = verification.reference(verification.fixture())[0][0]
        bad = dict(window, ema38="999")
        result = verification.compare([window], [bad, window])
        self.assertEqual(result["conflictingDuplicates"], 1)

    def test_missing_extra_and_wrong_values_are_reported(self):
        windows = verification.reference(verification.fixture())[0]
        result = verification.compare(windows[:2], [dict(windows[0], ema100="999"), windows[2]])
        self.assertEqual(result["missing"], 1)
        self.assertEqual(result["extra"], 1)
        self.assertEqual(result["mismatches"], 1)


if __name__ == "__main__":
    unittest.main()
