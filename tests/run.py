import os
import sys
from test_case import LOGS_DUMP_FILE_PATH
from tests import (
    OutputFiles,
    SigtermHandling,
    Concurrency,
    Json,
    ForcedExit,
    Batching,
    MemoryProfile,
    ServerShortReadWrite,
    ClientShortReadWrite,
)

TEST_CASES = [
    Json,
    ForcedExit,
    OutputFiles,
    Concurrency,
    MemoryProfile,
    SigtermHandling,
    ClientShortReadWrite,
    ServerShortReadWrite,
    Batching,
]
MESSAGE_PADDING = 32


def selected_test_cases():
    if len(sys.argv) < 2:
        return TEST_CASES

    requested_test = sys.argv[1].lower()
    matching_test_cases = [
        test_case
        for test_case in TEST_CASES
        if requested_test in test_case.__name__.lower()
        or requested_test in test_case.title.lower()
    ]

    if not matching_test_cases:
        available_tests = ", ".join(test_case.__name__ for test_case in TEST_CASES)
        raise ValueError(
            f"Unknown test '{sys.argv[1]}'. Available tests: {available_tests}"
        )

    return matching_test_cases


def main():
    try:
        test_cases = selected_test_cases()
    except ValueError as e:
        print(f"{e}", file=sys.stderr)
        return 1

    for test_case in test_cases:
        print(f"Testing {test_case.title.ljust(MESSAGE_PADDING, '.')}", end="")
        try:
            test_case.test()
            print("OK")
        except Exception as e:
            print("ERROR")
            print(f"{e}", file=sys.stderr)
            if os.path.isfile(LOGS_DUMP_FILE_PATH):
                print("Service Logs can be found at:", LOGS_DUMP_FILE_PATH, end="\n\n")
            print(f"HINT: {test_case.error_hint}", file=sys.stderr, end="\n\n")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
