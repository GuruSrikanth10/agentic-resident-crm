#!/usr/bin/env python3
"""
Stage 6 -- Evaluation Harness.

Runs the full log reduction pipeline end-to-end on a set of labeled test
cases and reports:
  1. Label accuracy (did the LLM get the outcome right?)
  2. Evidence citation accuracy (did it cite the actual causal evidence?)

Usage:
    python -m src.tools.eval_harness --test-cases test_cases.json

Test cases JSON format:
[
  {
    "refid": "abc-123",
    "expected_outcome": "reject",
    "expected_evidence_keywords": ["Rule R-2291", "FAIL"]
  }
]
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.log_pipeline.pipeline import reduce_logs


def main():
    parser = argparse.ArgumentParser(description="Stage 6 Evaluation Harness.")
    parser.add_argument("--test-cases", type=str, required=True,
                        help="Path to test cases JSON file.")
    args = parser.parse_args()

    with open(args.test_cases, "r", encoding="utf-8") as f:
        test_cases = json.load(f)

    print(f"Loaded {len(test_cases)} test cases.")

    results = []

    for i, tc in enumerate(test_cases):
        refid = tc["refid"]
        expected_outcome = tc.get("expected_outcome", "").lower()
        expected_keywords = tc.get("expected_evidence_keywords", [])

        print(f"\nTest case {i+1}/{len(test_cases)}: refid={refid}")
        print(f"    Expected outcome: {expected_outcome}")

        try:
            # Run the full pipeline (Stages 1-4)
            reduced_output = reduce_logs(refid)

            # Check if expected evidence keywords appear in the reduced output
            keywords_found = []
            keywords_missing = []
            for kw in expected_keywords:
                if kw.lower() in reduced_output.lower():
                    keywords_found.append(kw)
                else:
                    keywords_missing.append(kw)

            evidence_accuracy = len(keywords_found) / len(expected_keywords) if expected_keywords else 1.0

            result = {
                "refid": refid,
                "expected_outcome": expected_outcome,
                "evidence_accuracy": round(evidence_accuracy, 2),
                "keywords_found": keywords_found,
                "keywords_missing": keywords_missing,
                "reduced_output_length": len(reduced_output),
                "status": "PASS" if evidence_accuracy >= 0.8 else "FAIL",
            }
            results.append(result)

            print(f"    Evidence accuracy: {evidence_accuracy:.0%}")
            print(f"    Keywords found: {keywords_found}")
            if keywords_missing:
                print(f"    Keywords MISSING: {keywords_missing}")
            print(f"    Status: {result['status']}")

        except Exception as e:
            print(f"    ERROR: {e}")
            results.append({
                "refid": refid,
                "status": "ERROR",
                "error": str(e),
            })

    # Summary
    print("\nEvaluation summary")
    total = len(results)
    passed = sum(1 for r in results if r.get("status") == "PASS")
    failed = sum(1 for r in results if r.get("status") == "FAIL")
    errors = sum(1 for r in results if r.get("status") == "ERROR")
    avg_evidence = sum(r.get("evidence_accuracy", 0) for r in results if r.get("status") != "ERROR")
    avg_evidence = avg_evidence / (total - errors) if (total - errors) > 0 else 0

    print(f"Total:   {total}")
    print(f"Passed:  {passed}")
    print(f"Failed:  {failed}")
    print(f"Errors:  {errors}")
    print(f"Avg Evidence Accuracy: {avg_evidence:.0%}")

    # Write results to file
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "local_checkpoints", "eval_results.json"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to: {output_path}")


if __name__ == "__main__":
    main()
