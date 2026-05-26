# REPORT

이름:

---

## 01 · Flink 체크포인팅 — flink-basic

flink-conf.yaml에 설정한 값:

| 키 | 설정값 |
|------|--------|
| `execution.checkpointing.interval` | |
| `execution.checkpointing.mode` | |
| `state.backend` | |
| `taskmanager.numberOfTaskSlots` | |
| `parallelism.default` | |

시나리오 01 실행 중 Flink UI(http://localhost:8081)에서 본 것을 적으세요.

- Jobs → Running Jobs에서 parallelism이 설정한 값으로 나오나요?
- Jobs → Checkpoints에서 체크포인트가 interval마다 찍히고 있나요?

→ 

체크포인팅 설정이 없으면 잡 제출이 거부되는 이유를 한 줄로:

→ 

---

## 02 · Spark 배치 — spark-batch

result.json (`results/scenario_02.json`)에서 시나리오 02의 값:

| 항목 | 값 |
|------|-----|
| `rows_per_second` | |
| `shuffle_partitions_used` | |

이 시나리오는 가벼운 워밍업입니다 — Spark가 CSV를 읽어 groupBy로 집계하는 가장 기본 동작을 확인합니다.

---

## 03 · Stream vs Batch

시나리오 03 실행 후 result.json에서:

| 항목 | 값 |
|------|-----|
| `flink_latency_ms` | |
| `spark_job_time_ms` | |
| `latency_ratio` | |
| `flink_events_per_second` | |
| `spark_events_per_second` | |

**Latency 관점**: 같은 1000개 이벤트인데 Flink가 Spark보다 빠른 이유:

→ 

**Throughput 관점**: latency만 보면 Spark가 무조건 진 것 같지만, throughput도 봐주세요. Spark가 약하지 않은 이유, 그리고 실무에서 Spark가 더 적합한 상황은?

→ 

(힌트: latency = 한 건이 도착할 때까지 걸리는 시간 / throughput = 단위 시간당 처리량. 야간 대용량 배치에서는?)

**JVM 오버헤드 관점**: `spark_job_time_ms`에는 Spark 잡을 새로 띄울 때마다 발생하는 JVM 부트업 시간이 포함됩니다. Flink는 TaskManager가 이미 떠있는 상태에서 잡을 제출하므로 이 오버헤드가 없습니다. 이 차이를 이해했을 때, "스트리밍 엔진과 배치 엔진의 latency 차이"가 단순히 처리 방식 차이만은 아니라는 걸 알 수 있습니다. 한 줄로 정리해보세요.

→ 

---

## 04 · Spark Shuffle + DAG

시나리오 04는 자동으로 두 번 spark-submit을 실행합니다:
- **baseline**: `shuffle.partitions = 200`으로 측정
- **본 실행**: conf에 채운 값으로 측정

`results/scenario_04.json`에서:

| 항목 | 값 |
|------|-----|
| `shuffle_partitions_used` | |
| `job_time_ms` | |
| `baseline_time_ms` (200 기준) | |
| `speedup_factor` | |

**Spark UI 관찰** (http://localhost:8080): 실행 중인 잡 클릭 → Stages 탭에서 본 태스크 수:

→ 

**DAG Visualization** 열어서 본 내용:
- stage가 몇 개로 쪼개졌나요?
- 왜 groupBy 전후로 stage 경계가 생기나요? (힌트: 셔플)

→ 

`shuffle.partitions = 200`이 이 클러스터에서 비효율적인 이유 (코어 수 6 vs 태스크 수 200):

→ 

---

## 05 · Pipeline + Fault Tolerance

시나리오 05는 Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작하는지 확인합니다. `results/scenario_05.json`:

| 항목 | 값 |
|------|-----|
| `events_injected` | |
| `flink_output_sha256` | |
| `spark_output_sha256` | |
| `passed` | |

**여기서 한 번 정리해봅시다.** 지금까지 flink-conf.yaml에는 체크포인팅 설정을 채웠지만, spark-defaults.conf에는 그런 설정이 없습니다.

Spark는 fault tolerance(노드가 죽었을 때 복구)를 어떻게 제공하나요?

→ (힌트: 발제 자료의 RDD / Lineage)

Flink의 체크포인팅 방식과는 어떻게 다른가요? 각 접근의 장점/단점은?

→ 
