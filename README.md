# YBIGTA Spark & Flink 과제

**스트리밍과 배치, 같은 데이터를 처리하는 두 방식을 직접 체감합니다.**

---

## 과제 개요

Apache Flink(스트리밍)와 Apache Spark(배치)는 처리 모델이 근본적으로 다릅니다.

```
Flink  Kafka → 이벤트 도착 즉시 처리 → 결과       (낮은 latency)
Spark  CSV 전체 로드 → 한번에 집계 → 결과          (높은 throughput)
```

`student/conf/` 안의 설정 파일 두 개를 조정하고 `./run_all.sh`를 실행하면,
같은 데이터를 두 엔진으로 처리했을 때 얼마나 다른지 숫자로 확인할 수 있습니다.

---

## 빠른 시작

```bash
# 1. 설정 파일 수정 (student/README.md 참고)
vim student/conf/flink-conf.yaml
vim student/conf/spark-defaults.conf

# 2. Docker 이미지 빌드 (최초 1회, 5~10분)
./scripts/cluster.sh build

# 3. 클러스터 시작
./scripts/cluster.sh init

# 4. 전체 시나리오 실행
./run_all.sh

# 5. 결과 확인
cat results/result.json
```

### 준비 사항

- Docker Desktop (최신) — **RAM 6GB 이상 할당 필수**
  - Settings → Resources → Memory에서 조정
- Docker Compose v2 이상
- 디스크 여유 공간 10GB 이상
- 빌드 시 인터넷 연결 필요

---

## 수강생이 해야 할 것

`student/` 안에서만 작업합니다.

| 파일 | 역할 |
|------|------|
| `student/conf/flink-conf.yaml` | Flink 체크포인팅 / 병렬성 설정 |
| `student/conf/spark-defaults.conf` | Spark shuffle 파티션 수 조정 |
| `student/docker-compose.student.yml` | 컨테이너별 메모리 리소스 조정 (선택) |

수정하면 안 되는 것: `docker-compose.yml`, `jobs/`, `scenarios/`, `scripts/`

설정 방법은 [`student/README.md`](student/README.md)를 참고하세요.

---

## 검증 시나리오

| # | 이름 | 내용 | 핵심 메트릭 |
|---|------|------|------------|
| 01 | `flink-basic` | Kafka 이벤트 실시간 처리 검증 | `processing_latency_ms` |
| 02 | `spark-batch` | CSV 배치 집계 검증 | `job_time_ms`, `rows_per_second` |
| 03 | `stream-vs-batch` | 같은 데이터, 두 엔진 처리 시간 비교 | **`latency_ratio`** |
| 04 | `spark-shuffle` | Shuffle 파티션 수에 따른 속도 차이 | `speedup_factor` |
| 05 | `pipeline` | Kafka → Flink → Spark 엔드투엔드 | 전체 흐름 검증 |

**제출 기준: 5개 시나리오 모두 `"passed": true`**

---

## 디렉터리 구조

```
YBIGTA_SPFL_hw/
├── docker-compose.yml
├── run_all.sh
├── jobs/                     # Flink / Spark 잡 코드
├── scenarios/                # 시나리오별 실행 스크립트
├── scripts/                  # 클러스터 제어 스크립트
├── student/                  # ★ 이 안에서만 작업 ★
│   ├── conf/
│   │   ├── flink-conf.yaml
│   │   └── spark-defaults.conf
│   └── docker-compose.student.yml
├── results/                  # run_all.sh 결과 JSON
└── submissions/              # 제출 파일 위치
```

---

## 채점 기준

| 항목 | 기준 |
|------|------|
| 1순위 | `scenarios_passed` (최대 5) |
| 2순위 | `penalty_ms` (낮을수록 유리 — 실패 시나리오 소요 시간 합산) |

---

## 제출 방법

```bash
./scripts/prepare_submission.sh <이름>
git commit -m "submit: <이름>"
git push
```

스크립트가 `results/result.json`과 `student/conf/`를 `submissions/<이름>/`으로 복사하고 `git add`까지 처리합니다.

PR에는 `submissions/<이름>/result.json`과 `submissions/<이름>/conf/*`만 포함되어야 합니다.

---

## 클러스터 관리

```bash
./scripts/cluster.sh build    # 이미지 빌드
./scripts/cluster.sh init     # 클러스터 시작 + 테스트 데이터 적재
./scripts/cluster.sh status   # 상태 확인
./scripts/cluster.sh down     # 중지 (볼륨 유지)
./scripts/cluster.sh clean    # 완전 초기화
```

## 디버깅

```bash
open http://localhost:8081    # Flink UI
open http://localhost:8080    # Spark UI
docker logs -f flink-jm      # Flink JobManager 로그
```
