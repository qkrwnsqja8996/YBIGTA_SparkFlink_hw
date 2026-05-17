# 과제 명세서 — Spark & Flink 스트리밍 vs 배치

**과목**: YBIGTA 빅데이터 엔지니어링
**주제**: Apache Flink(스트리밍)와 Apache Spark(배치)의 처리 방식 차이 체감
**제출 기준**: 5개 시나리오 모두 통과 (`"scenarios_passed": 5`) + `results/result.json` 제출

---

## 1. 이 과제에서 배우는 것

Flink와 Spark는 둘 다 데이터를 처리하는 프레임워크지만, **처리 모델이 근본적으로 다릅니다.**

```
Flink (스트리밍)
  Kafka → [이벤트 1 도착] → 즉시 처리 → 출력
          [이벤트 2 도착] → 즉시 처리 → 출력
          [이벤트 3 도착] → 즉시 처리 → 출력
  핵심: 데이터가 오는 즉시 처리. 지연(latency)이 낮다.

Spark (배치)
  CSV 파일 전체 로드 → 한번에 집계 → 출력
  [row1, row2, ..., row10000] → groupBy → [결과]
  핵심: 데이터를 모아서 한번에 처리. 처리량(throughput)이 높다.
```

시나리오 03(stream-vs-batch)에서 **같은 1,000개 이벤트**를 두 엔진으로 처리하면
결과가 얼마나 다른 시간에 나오는지 직접 확인합니다.

---

## 2. 시스템 아키텍처

```
┌─────────────┐   events    ┌──────────────────────────────────────┐
│  ZooKeeper  │◄───────────►│               Kafka                  │
└─────────────┘             └──────────────┬───────────────────────┘
                                           │
                                           ▼
                            ┌──────────────────────────┐
                            │      Flink Cluster        │
                            │  ┌────────────────────┐  │  :8081
                            │  │   JobManager (JM)   │  │
                            │  └────────────────────┘  │
                            │   TM1    TM2    TM3       │
                            └──────────────┬────────────┘
                                           │ /data/flink-output/
                                           ▼
                            ┌──────────────────────────┐
                            │      Spark Cluster        │
                            │  ┌────────────────────┐  │  :8080
                            │  │      Master         │  │
                            │  └────────────────────┘  │
                            │  Worker1 Worker2 Worker3  │
                            └──────────────────────────┘

공유 볼륨: pipeline-data → /data
  /data/checkpoints/   ← Flink 체크포인트
  /data/flink-output/  ← Flink 잡 결과
  /data/spark-input/   ← Spark 입력 데이터
  /data/spark-output/  ← Spark 잡 결과
```

---

## 3. 사전 요구사항

| 요구사항 | 버전 / 사양 |
|----------|------------|
| Docker Desktop | 최신 버전 (RAM 8GB 이상 할당) |
| Docker Compose | v2 이상 |
| Python | 3.8 이상 |

---

## 4. 수정해야 할 파일

`student/conf/` 안의 파일 두 개만 수정합니다.

```
student/conf/
├── flink-conf.yaml       ← Flink 설정 (TODO 항목 주석 해제)
└── spark-defaults.conf   ← Spark 설정 (TODO 항목 주석 해제)
```

### flink-conf.yaml 필수 설정

```yaml
execution.checkpointing.interval: 5000
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: file:///data/checkpoints
state.backend: hashmap
taskmanager.numberOfTaskSlots: 2
parallelism.default: 2
```

### spark-defaults.conf 필수 설정

```
spark.sql.shuffle.partitions    4
spark.default.parallelism       4
```

> **왜 shuffle.partitions를 4로?**
> Spark 기본값 200은 수백 대 클러스터를 위한 설정입니다.
> 소규모 데이터에 200개 파티션을 만들면 대부분 빈 파티션이라
> 스케줄링 오버헤드만 생깁니다. 시나리오 04에서 직접 확인해보세요.

---

## 5. 실행 방법

```bash
# 1. 설정 파일 수정 (위 4 참고)

# 2. Docker 이미지 빌드 (최초 1회, 5~10분 소요)
./scripts/cluster.sh build

# 3. 클러스터 시작
./scripts/cluster.sh init

# 4. 전체 시나리오 실행
./run_all.sh

# 5. 결과 확인
cat results/result.json
```

---

## 6. 시나리오 상세

총 **5개 시나리오**를 순서대로 실행합니다.

### 시나리오 01 — Flink 기본 스트리밍 (`flink-basic`) ⭐

| 항목 | 내용 |
|------|------|
| **목표** | Flink가 Kafka 이벤트를 실시간으로 처리하는지 확인 |
| **흐름** | Kafka에 1,000개 이벤트 주입 → Flink 처리 → 출력 해시 검증 |
| **통과 조건** | 출력 파일 SHA-256 해시 일치 |
| **결과 메트릭** | `processing_latency_ms`, `events_per_second` |

### 시나리오 02 — Spark 기본 배치 (`spark-batch`) ⭐

| 항목 | 내용 |
|------|------|
| **목표** | Spark가 CSV를 읽어 올바르게 배치 집계하는지 확인 |
| **흐름** | `events.csv` 읽기 → 집계 → 출력 해시 검증 |
| **통과 조건** | 출력 파일 SHA-256 해시 일치 |
| **결과 메트릭** | `rows_per_second`, `job_time_ms`, `shuffle_partitions_used` |

### 시나리오 03 — 스트리밍 vs 배치 비교 (`stream-vs-batch`) ⭐⭐

| 항목 | 내용 |
|------|------|
| **목표** | 같은 1,000개 이벤트를 Flink와 Spark로 처리, 응답 시간 비교 |
| **흐름** | Flink로 Kafka 스트림 처리 → `flink_latency_ms` 측정 → Spark로 동일 데이터 배치 처리 → `spark_job_time_ms` 측정 |
| **통과 조건** | Flink 출력 해시 일치 AND Spark 출력 행 수 정확 |
| **핵심 메트릭** | `latency_ratio` = spark_job_time_ms / flink_latency_ms |

결과 예시:
```json
{
  "flink_latency_ms": 210,
  "spark_job_time_ms": 9800,
  "latency_ratio": 46.7
}
```
→ 같은 데이터인데 Spark는 첫 결과를 내기까지 Flink보다 47배 걸렸다.

### 시나리오 04 — Spark Shuffle 파티션 함정 (`spark-shuffle`) ⭐⭐

| 항목 | 내용 |
|------|------|
| **목표** | 파티션 설정이 배치 성능에 얼마나 영향을 주는지 확인 |
| **흐름** | 50,000행 데이터 생성 → partition=200(기본값)으로 실행(baseline) → 학생 설정값으로 실행 → 속도 비교 |
| **통과 조건** | 60초 이내 완료 |
| **핵심 메트릭** | `speedup_factor`, `baseline_time_ms` vs `job_time_ms` |

결과 예시:
```json
{
  "baseline_time_ms": 45000,
  "job_time_ms": 3200,
  "speedup_factor": 14.1,
  "shuffle_partitions_used": 4
}
```
→ 파티션 수 하나 바꿨더니 14배 빨라졌다.

### 시나리오 05 — 엔드투엔드 파이프라인 (`pipeline`) ⭐⭐⭐

| 항목 | 내용 |
|------|------|
| **목표** | Kafka → Flink → Spark 전체 파이프라인이 정상 동작하는지 확인 |
| **흐름** | 2,000개 이벤트 주입 → Flink 처리 → 결과를 CSV로 변환 → Spark 집계 → 최종 출력 검증 |
| **통과 조건** | Flink 출력 해시 + Spark 출력 해시 모두 일치 |

---

## 7. 채점 기준

| 항목 | 기준 |
|------|------|
| **1순위** | `scenarios_passed` (최대 5) |
| **2순위** | `penalty_ms` (낮을수록 유리 — 실패한 시나리오의 소요 시간 합산) |

| 시나리오 | 난이도 | 핵심 설정 |
|----------|--------|----------|
| 01 flink-basic | ⭐ | `checkpointing.interval` |
| 02 spark-batch | ⭐ | `spark.sql.shuffle.partitions` |
| 03 stream-vs-batch | ⭐⭐ | 위 설정 모두 |
| 04 spark-shuffle | ⭐⭐ | `spark.sql.shuffle.partitions <= 8` |
| 05 pipeline | ⭐⭐⭐ | 모든 설정 |

---

## 8. 제출 방법

```bash
cp results/result.json submissions/
```

**제출 파일:**
- `student/conf/flink-conf.yaml`
- `student/conf/spark-defaults.conf`
- `submissions/result.json`

---

## 9. 디버깅 가이드

```bash
# Flink UI
open http://localhost:8081

# Spark UI
open http://localhost:8080

# Flink JobManager 로그
docker logs -f flink-jm

# Kafka 토픽 목록
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# 클러스터 완전 초기화
./scripts/cluster.sh down && ./scripts/cluster.sh clean && ./scripts/cluster.sh build && ./scripts/cluster.sh init
```

| 오류 상황 | 원인 | 해결 |
|-----------|------|------|
| 시나리오 01/03 실패 | checkpointing 미설정 | `flink-conf.yaml` TODO 항목 설정 |
| 시나리오 04 타임아웃 | shuffle.partitions = 200 | `spark.sql.shuffle.partitions = 4` |
| 시나리오 03 `latency_ratio` 낮음 | parallelism 낮음 | `parallelism.default: 2` 이상 설정 |

---

## 10. 핵심 개념

### Flink vs Spark: 언제 처리하는가?

| 항목 | Flink (스트리밍) | Spark (배치) |
|------|----------------|-------------|
| 처리 시작 | 이벤트 도착 즉시 | 전체 데이터 수집 후 |
| 첫 결과 시점 | 수백 ms | 수초~수십초 (JVM 기동 포함) |
| 강점 | 낮은 latency, 실시간 | 높은 throughput, 대용량 |
| 상태 관리 | Stateful (ValueState) | Stateless (DataFrame) |
| 과제 내 사용 | 시나리오 01, 03, 05 | 시나리오 02, 03, 04, 05 |

### Spark Shuffle Partitions란?

Spark의 `groupBy`, `join`은 내부적으로 데이터를 파티션 단위로 재분배합니다.

```
기본값 200 파티션 → 소규모 데이터 → 빈 파티션 196개 + 데이터 있는 파티션 4개
                                    → 스케줄링 오버헤드 × 200

최적값 4 파티션   → 소규모 데이터 → 파티션 4개 모두 활용
                                    → 빠른 처리
```

시나리오 04의 `speedup_factor`로 직접 확인할 수 있습니다.

---

*본 과제는 YBIGTA 빅데이터 엔지니어링 스터디용으로 제작되었습니다.*
