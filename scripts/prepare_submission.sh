#!/usr/bin/env bash
# 제출 파일 준비 스크립트
# Usage: scripts/prepare_submission.sh <이름>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: scripts/prepare_submission.sh <이름>

results/result.json 과 student/conf/ 를 submissions/<이름>/ 으로 복사하고
git add 까지 수행합니다.

이후 아래 명령으로 제출하세요:
  git commit -m "submit: <이름>"
  git push
USAGE
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }
[[ $# -ne 1 ]] && { usage; exit 2; }

NAME="$1"

if [[ "${NAME}" == *"/"* || "${NAME}" == "." || "${NAME}" == ".." || -z "${NAME}" ]]; then
  echo "잘못된 이름: ${NAME}" >&2
  echo "슬래시 없는 폴더명을 사용하세요." >&2
  exit 1
fi

RESULT_FILE="${ROOT_DIR}/results/result.json"
STUDENT_CONF_DIR="${ROOT_DIR}/student/conf"
DEST_DIR="${ROOT_DIR}/submissions/${NAME}"
DEST_RESULT="${DEST_DIR}/result.json"
DEST_CONF_DIR="${DEST_DIR}/conf"

if [[ ! -f "${RESULT_FILE}" ]]; then
  echo "오류: ${RESULT_FILE} 없음" >&2
  echo "먼저 ./run_all.sh 를 실행하세요." >&2
  exit 1
fi

# 결과 JSON 유효성 검증
python3 "${ROOT_DIR}/scripts/validate_submission_result.py" "${RESULT_FILE}"

mkdir -p "${DEST_DIR}"
cp "${RESULT_FILE}" "${DEST_RESULT}"

rm -rf "${DEST_CONF_DIR}"
mkdir -p "${DEST_CONF_DIR}"
cp -R "${STUDENT_CONF_DIR}/." "${DEST_CONF_DIR}/"

git -C "${ROOT_DIR}" add -- \
  "submissions/${NAME}/result.json" \
  "submissions/${NAME}/conf"

echo ""
echo "제출 준비 완료: submissions/${NAME}/"
echo ""
echo "staged 파일:"
git -C "${ROOT_DIR}" diff --cached --name-only
echo ""
echo "다음 명령으로 제출:"
echo "  git commit -m \"submit: ${NAME}\""
echo "  git push"
