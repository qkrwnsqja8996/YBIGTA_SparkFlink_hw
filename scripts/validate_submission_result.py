#!/usr/bin/env python3
"""PR 검증용 result.json 유효성 검사."""

from __future__ import annotations

import json
import sys
from pathlib import Path

VALID_SCENARIO_IDS = {"01", "02", "03", "04", "05"}


def fail(msg: str) -> None:
    print(f"ERROR: {msg}")
    sys.exit(1)


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_submission_result.py <result.json 경로>")

    path = Path(sys.argv[1])
    if not path.is_file():
        fail(f"파일 없음: {path}")

    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as e:
        fail(f"JSON 파싱 오류: {e}")

    if not isinstance(data, dict):
        fail("최상위 JSON 값은 object 여야 합니다")

    for key in ("scenarios_passed", "scenarios_total", "penalty_ms", "scenarios"):
        if key not in data:
            fail(f"필수 필드 없음: {key}")

    scenarios = data["scenarios"]
    if not isinstance(scenarios, list):
        fail("scenarios 는 배열이어야 합니다")

    # 시나리오별 검증
    seen_ids: set[str] = set()
    actual_passed = 0
    penalty_sum = 0

    for i, sc in enumerate(scenarios):
        if not isinstance(sc, dict):
            fail(f"scenarios[{i}] 는 object 여야 합니다")

        sid = sc.get("id")
        if not isinstance(sid, str):
            fail(f"scenarios[{i}].id 가 없거나 문자열이 아닙니다")
        if sid in seen_ids:
            fail(f"scenarios[{i}].id 중복: {sid}")
        seen_ids.add(sid)

        passed = sc.get("passed")
        if not isinstance(passed, bool):
            fail(f"scenarios[{i}].passed 는 boolean 이어야 합니다")

        elapsed = sc.get("elapsed_ms", 0)
        if not isinstance(elapsed, int) or isinstance(elapsed, bool):
            fail(f"scenarios[{i}].elapsed_ms 는 정수여야 합니다")

        if passed:
            actual_passed += 1
        else:
            penalty_sum += elapsed

    # 집계 일관성 확인
    claimed_passed = data["scenarios_passed"]
    if not isinstance(claimed_passed, int) or isinstance(claimed_passed, bool):
        fail("scenarios_passed 는 정수여야 합니다")
    if claimed_passed != actual_passed:
        fail(
            f"scenarios_passed 불일치\n"
            f"  선언값: {claimed_passed}\n"
            f"  실제값: {actual_passed}"
        )

    claimed_penalty = data["penalty_ms"]
    if not isinstance(claimed_penalty, int) or isinstance(claimed_penalty, bool):
        fail("penalty_ms 는 정수여야 합니다")
    if claimed_penalty != penalty_sum:
        fail(
            f"penalty_ms 불일치 (실패한 시나리오의 elapsed_ms 합계여야 함)\n"
            f"  선언값: {claimed_penalty}\n"
            f"  계산값: {penalty_sum}"
        )

    total = data["scenarios_total"]
    print(f"통과: {actual_passed}/{total}")
    print(f"penalty_ms: {claimed_penalty:,}")
    print("result.json 검증 통과")


if __name__ == "__main__":
    main()
