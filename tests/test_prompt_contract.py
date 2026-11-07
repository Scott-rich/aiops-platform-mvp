"""The compact prompt must not change the enforced diagnosis protocol."""
import copy
import unittest

from app.analysis_schema import json_schema, validate_analysis
from app.diagnosis import SYSTEM, prompt_schema


class PromptContractTests(unittest.TestCase):
    def test_compact_description_preserves_required_and_nonempty_rules(self):
        source = json_schema()
        before = copy.deepcopy(source)
        compact = prompt_schema(source)
        self.assertEqual(source, before)
        self.assertEqual(compact["required"], source["required"])
        hypothesis = compact["$defs"]["Hypothesis"]
        self.assertEqual(hypothesis["required"], source["$defs"]["Hypothesis"]["required"])
        self.assertEqual(hypothesis["properties"]["supporting_evidence_ids"]["minItems"], 1)
        self.assertEqual(hypothesis["properties"]["confidence"]["enum"], ["low", "medium", "high"])
        self.assertEqual(compact["properties"]["uncertainties"]["minItems"], 1)
        self.assertEqual(compact["properties"]["summary"]["minLength"], 1)
        self.assertFalse(compact["additionalProperties"])
        self.assertIn("unknown", compact["properties"]["diagnosis_status"]["enum"])

    def test_full_validation_still_rejects_size_violation(self):
        valid = {"analysis_schema_version": 2, "diagnosis_status": "unknown",
                 "summary": "Insufficient evidence", "involved_services": [], "hypotheses": [],
                 "uncertainties": ["Internal cause unobserved"],
                 "verification_steps": ["Collect internal metrics"],
                 "recommendations": ["Review evidence before acting"]}
        self.assertEqual(validate_analysis(valid, [])["diagnosis_status"], "unknown")
        for change in ({"summary": "x" * 3001}, {"recommendations": ["step"] * 13}):
            with self.assertRaisesRegex(ValueError, "diagnosis_schema_invalid"):
                validate_analysis({**valid, **change}, [])
        self.assertIn("against_evidence_ids=[]", SYSTEM)
        self.assertIn("hypotheses=[]", SYSTEM)


if __name__ == "__main__":
    unittest.main()
