"""Regression for grammar-induced punctuation being mistaken for useful prose."""
import unittest
from app.analysis_schema import validate_analysis
from tests.model_support import analysis, records


class DiagnosticTextTests(unittest.TestCase):
    def test_punctuation_only_explanations_are_rejected(self):
        for text in ('],', '—', '…', '  ', '"}]'):
            value=analysis();value['hypotheses'][0]['uncertainty']=[text]
            with self.subTest(text=text),self.assertRaisesRegex(ValueError,'diagnosis_schema_invalid'):
                validate_analysis(value,records())

    def test_multilingual_unknown_and_real_steps_remain_valid(self):
        for text in ('无法确定内部根因，需要补充进程指标。','Insufficient evidence; inspect dependency logs.'):
            value=analysis(unknown=True);value['uncertainties']=[text]
            self.assertEqual(validate_analysis(value,records())['uncertainties'],[text])
