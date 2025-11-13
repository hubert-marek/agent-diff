#!/usr/bin/env python3
import os
import sys
import json
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from src.platform.db.schema import Test, TestRun, TestSuite, TestMembership


def _normalize_expected_output(
    test_data: dict, suite_ignore_fields: dict | None = None
) -> dict:
    # Case 1: Test has explicit expected_output dict
    if "expected_output" in test_data and isinstance(
        test_data["expected_output"], dict
    ):
        result = dict(test_data["expected_output"])  # Copy to avoid mutation

        # Merge suite-level ignore_fields if present
        if suite_ignore_fields is not None:
            if "ignore_fields" not in result:
                result["ignore_fields"] = {}

            # Merge global ignore fields
            if "global" in suite_ignore_fields:
                existing_global = result["ignore_fields"].get("global", [])
                # Combine and deduplicate
                combined = list(set(existing_global + suite_ignore_fields["global"]))
                result["ignore_fields"]["global"] = combined

            # Merge entity-specific ignore fields
            for key, value in suite_ignore_fields.items():
                if key != "global":
                    existing = result["ignore_fields"].get(key, [])
                    combined = list(set(existing + value))
                    result["ignore_fields"][key] = combined

        return result

    # Case 2: Test has assertions list (shorthand)
    assertions = test_data.get("assertions")
    if isinstance(assertions, list):
        result: dict = {"assertions": assertions}
        if suite_ignore_fields is not None:
            result["ignore_fields"] = dict(suite_ignore_fields)
        return result

    return {}


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    examples_root = Path(__file__).resolve().parent.parent.parent / "examples"

    # Discover all test suite JSON files
    test_suite_files = list(examples_root.glob("*/testsuites/*.json"))

    if not test_suite_files:
        print("No test suite files found in examples/*/testsuites/")
        return

    engine = create_engine(db_url)

    with Session(engine) as session:
        for test_file in test_suite_files:
            print(
                f"Loading test suite from {test_file.relative_to(examples_root.parent)}"
            )

            with open(test_file) as f:
                data = json.load(f)

            suite_name = data.get("name", test_file.stem)
            suite_description = data.get(
                "description", f"Test suite from {test_file.name}"
            )
            owner = data.get("owner", "dev-user")

            existing_suite = (
                session.query(TestSuite)
                .filter(TestSuite.name == suite_name, TestSuite.owner == owner)
                .one_or_none()
            )

            if existing_suite:
                print(f"  → Suite '{suite_name}' exists, refreshing tests")
                memberships = (
                    session.query(TestMembership)
                    .filter(TestMembership.test_suite_id == existing_suite.id)
                    .all()
                )
                test_ids = [m.test_id for m in memberships]
                for membership in memberships:
                    session.delete(membership)
                if test_ids:
                    session.query(TestRun).filter(TestRun.test_id.in_(test_ids)).delete(
                        synchronize_session=False
                    )
                    session.query(Test).filter(Test.id.in_(test_ids)).delete(
                        synchronize_session=False
                    )
                session.query(TestRun).filter(
                    TestRun.test_suite_id == existing_suite.id
                ).delete(synchronize_session=False)
                test_suite = existing_suite
                test_suite.description = suite_description
            else:
                test_suite = TestSuite(
                    name=suite_name,
                    description=suite_description,
                    owner=owner,
                    visibility="public",
                )
                session.add(test_suite)
                session.flush()

            # Create test suite for dev user
            test_count = 0
            suite_ignore_fields = data.get("ignore_fields")
            for test_data in data.get("tests", []):
                test = Test(
                    name=test_data["name"],
                    prompt=test_data["prompt"],
                    type=test_data["type"],
                    expected_output=_normalize_expected_output(
                        test_data, suite_ignore_fields
                    ),
                    template_schema=test_data.get("seed_template"),
                    impersonate_user_id=test_data.get("impersonate_user_id"),
                )
                session.add(test)
                session.flush()

                membership = TestMembership(
                    test_id=test.id,
                    test_suite_id=test_suite.id,
                )
                session.add(membership)
                test_count += 1

            print(f"  → Loaded {test_count} tests in suite '{test_suite.name}'")

        session.commit()
        print(f"\nSuccessfully seeded {len(test_suite_files)} test suite(s)")


if __name__ == "__main__":
    main()
