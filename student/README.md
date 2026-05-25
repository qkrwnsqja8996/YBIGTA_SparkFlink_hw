# 클러스터 구조

`./scripts/cluster.sh init`을 실행하면 아래 컨테이너들이 뜹니다.

| 컨테이너 | 역할 |
|----------|------|
| flink-jm | Flink JobManager — 잡 스케줄링, 슬롯 관리 |
| flink-tm1~3 | Flink TaskManager 3대 — 실제 태스크 실행 |
| spark-master | Spark Master — 잡 접수, Executor 관리 |
| spark-worker1~3 | Spark Worker 3대 — 실제 Executor 실행 |
| kafka | 시나리오 01·03·05에서 이벤트 스트림으로 사용 |

클러스터가 정상적으로 뜨면 아래 UI에서 상태를 확인할 수 있습니다.
- Flink: http://localhost:8081
- Spark: http://localhost:8080

---

## 설정 파일

### conf/flink-conf.yaml

> **파일 문법**
> `키: 값` 형식. `#`으로 막힌 줄에서 `#`을 지우고 값을 채웁니다.
> ```yaml
> # execution.checkpointing.interval:
> execution.checkpointing.interval: 10000
> ```

**Checkpointing**

> Hint
> Flink는 스트리밍 처리 중 집계값·세션 정보 같은 상태(state)를 메모리에 들고 있습니다.
> 분산 시스템에서 노드 장애는 항상 발생하고, 장애가 나면 메모리가 날아갑니다.
> 그래서 Flink는 주기적으로 상태 전체를 외부 경로에 스냅샷으로 찍어두고,
> 장애 발생 시 가장 최근 스냅샷에서 복구합니다.
>
> 같은 이벤트가 두 번 집계되면 안 되는 상황이라면 mode는 어떻게 해야 할까요?
>
> **이 설정이 빠지면**: JobManager가 잡 제출 자체를 거부합니다. 시나리오 01이 즉시 실패합니다.

| 키 | 설명 | 형식 예시 |
|----|------|-----------|
| `execution.checkpointing.interval` | 스냅샷 주기 (ms) | `10000` (= 10초) |
| `execution.checkpointing.mode` | 중복 처리 허용 여부 | `EXACTLY_ONCE` / `AT_LEAST_ONCE` |
| `state.checkpoints.dir` | 스냅샷 저장 경로 | `file:///data/flink-checkpoints` |
| `state.backend` | 상태 저장 방식 | `hashmap` (메모리) / `rocksdb` (디스크) |

**병렬성**

> Hint
> Flink는 잡을 여러 태스크로 나눠 TaskManager의 슬롯에 분배합니다.
> 가용 슬롯 총합 = TM 수 × TM당 슬롯 수이며,
> parallelism이 이 값을 넘으면 잡이 PENDING 상태에서 멈춥니다.
> 이 클러스터의 TM은 3대, TM당 메모리는 1728m입니다.
>
> **잘못 설정하면**: parallelism이 가용 슬롯보다 크면 잡이 `SCHEDULED`에서 진행되지 않고 timeout으로 실패합니다.

| 키 | 설명 | 힌트 |
|----|------|------|
| `taskmanager.numberOfTaskSlots` | TM 1대당 슬롯 수 | 슬롯이 많을수록 메모리를 잘게 나눔. `1` 또는 `2` 권장 |
| `parallelism.default` | 기본 병렬 태스크 수 | `TM 수(3) × numberOfTaskSlots` 이하 |

---

### conf/spark-defaults.conf

> **파일 문법**
> `키    값` 형식으로 공백으로 구분합니다.
> ```
> # spark.default.parallelism
> spark.default.parallelism    6
> ```

**Shuffle Partition**

> Hint
> groupBy/join을 실행하면 Spark는 같은 키의 데이터를 한 파티션으로 모아야 합니다.
> 이 과정이 셔플이고, 파티션 하나당 태스크 하나가 Executor에 할당됩니다.
> 파티션이 클러스터 코어 수보다 지나치게 많으면
> 실제 연산보다 태스크 스케줄링 오버헤드가 더 커집니다.
> 이 클러스터는 worker 3대 × 코어 2개 = 총 6코어입니다.
>
> **현재 200 그대로 돌리면**: 코어 6개가 태스크 200개를 순차 처리하면서 스케줄링 오버헤드가 폭증해 시나리오 04가 실패합니다.

| 키 | 현재 값 | 형식 예시 |
|----|---------|-----------|
| `spark.sql.shuffle.partitions` | `200` → 변경 필요 | `spark.sql.shuffle.partitions    6` |
| `spark.default.parallelism` | 미설정 | `spark.default.parallelism    6` |

---

## 클러스터 시작

설정 파일을 채운 다음에 `./scripts/cluster.sh init`을 실행합니다.
빈 설정으로 먼저 띄우면 슬롯 수 같은 값이 default로 떠서 conf 수정이 반영되지 않습니다.

> conf를 또 수정하면 `./scripts/cluster.sh init`을 다시 실행해야 반영됩니다.

---

## 결과 파일 구조

시나리오를 실행하면 `results/` 아래에 두 종류의 파일이 생성됩니다.

| 파일 | 내용 |
|------|------|
| `results/scenario_01.json` ~ `scenario_05.json` | 각 시나리오의 상세 수치 |
| `results/result.json` | 전체 시나리오 통과 여부 요약 |

REPORT.md를 작성할 때는 해당 시나리오의 `scenario_XX.json`을 열어보면 됩니다.

---

## 시나리오별 진행

> **권장 방식**: 시나리오를 **하나씩** 실행하고 UI 관찰 후 `REPORT.md`의 해당 섹션을 바로 작성하세요.
> 자동 배치 실행(`./run_all.sh`)은 **마지막 최종 검증용**입니다.

```bash
./run_all.sh --scenarios 1   # 시나리오 01만
./run_all.sh --scenarios 4   # 시나리오 04만
./run_all.sh                 # 전부 (제출 직전 검증)
```

---

### 01 — flink-basic

Kafka에서 1000개 이벤트를 읽어 Flink가 실시간으로 처리합니다.

실행하면서 **http://localhost:8081** 을 열어보세요.

- **Jobs → Running Jobs**: 내가 설정한 parallelism대로 잡이 올라왔나요?
- **Jobs → Checkpoints**: 설정한 interval마다 체크포인트가 찍히고 있나요?

→ 통과되면 **REPORT.md `01` 섹션** 작성.

---

### 02 — spark-batch

CSV 파일을 읽어 groupBy로 집계합니다.

실행하면서 **http://localhost:8080** 을 열어보세요.

- **Completed Applications**: 방금 끝난 잡이 보이나요?
- 잡 클릭 → **Stages** 탭: stage가 몇 개인가요? (CSV 읽기 + 집계 = 2개)
- Stage 클릭: 태스크 수는 `shuffle.partitions` 값과 관계 있습니다.

→ **REPORT.md `02` 섹션** 작성.

---

### 03 — stream-vs-batch

같은 데이터를 Flink와 Spark 양쪽으로 처리해 **latency와 throughput을 비교**합니다.

실행 후 `results/result.json`에서 두 엔진의 수치를 찾아보세요.

- Flink는 latency가 짧음 (Spark: 모았다가 한 번에 / Flink: 즉시 처리)
- 근데 throughput도 따로 보세요. 대용량 야간 배치라면 어느 쪽이 유리할까요?

> **참고**: `spark_job_time_ms`에는 Spark 잡을 새로 띄울 때마다 발생하는 **JVM 부트업 오버헤드**가 포함됩니다 (보통 수 초). Flink는 TaskManager가 이미 떠있는 상태에서 잡만 제출하므로 이 오버헤드가 없습니다. 이건 "Spark가 본질적으로 느리다"가 아니라 **"배치 엔진은 매번 잡을 새로 띄우는 구조라 짧은 작업에서 손해를 본다"** 는 뜻입니다.

→ **REPORT.md `03` 섹션**에서 두 관점을 모두 정리.

---

### 04 — spark-shuffle

shuffle partition 수가 성능에 미치는 영향과 Spark의 **DAG 구조**를 같이 봅니다.

시나리오 자체가 매번 **baseline(200) + 본인 설정값**으로 두 번 spark-submit을 실행합니다. result.json의 `baseline_time_ms`와 `job_time_ms`를 비교할 수 있습니다.

**진행:**
1. `spark.sql.shuffle.partitions`를 클러스터 코어 수에 맞춰 채우고 `./scripts/cluster.sh init` → `./run_all.sh --scenarios 4` 실행
2. **http://localhost:8080** → 실행 중인 잡 클릭 → **Stages** 탭에서 태스크 수가 `shuffle.partitions` 값과 일치하는지 확인
3. **DAG Visualization**도 열어 stage가 어떻게 나뉘는지 보기 (groupBy 전후 = 셔플 경계)

> `speedup_factor`가 1보다 작게 나올 수 있습니다 — 이 데이터셋 규모에선 두 번째 spark-submit의 JVM 재기동 비용이 partition 차이를 묻어버립니다. 학습 포인트는 "**파티션 수 = 태스크 수**"이고, Stages 탭에서 직접 확인하면 됩니다.

> 시나리오를 통과시키려면 `shuffle.partitions`가 8 이하여야 합니다.

→ **REPORT.md `04` 섹션**에 partition 수 비교 + DAG 관찰 작성.

---

### 05 — pipeline

Kafka → Flink → Spark 전체 파이프라인이 end-to-end로 동작합니다.

두 엔진의 **fault tolerance 철학 차이**도 같이 정리합니다.

- flink-conf.yaml에는 체크포인팅 설정을 채웠음
- spark-defaults.conf에는 그런 설정이 없음
- Spark는 어떻게 fault tolerance를 제공하나? (힌트: 발제의 RDD lineage)

→ **REPORT.md `05` 섹션**에서 두 엔진의 fault tolerance 접근을 비교 정리.

---

## 최종 검증 & 제출

모든 시나리오가 통과하고 REPORT가 다 작성됐으면 마지막 검증:

```bash
./run_all.sh
```

5/5가 모두 `passed: true`인지 `results/result.json`에서 확인 후 제출:

```bash
./scripts/prepare_submission.sh <이름>
git commit -m "submit: <이름>"
git push
```

> **제출 방법**
> 1. GitHub에서 이 저장소를 **Fork** 합니다.
> 2. Fork한 저장소를 클론해서 과제를 진행합니다.
> 3. 위 명령어로 커밋·푸시한 뒤, 원본 저장소로 **Pull Request**를 올립니다.

---

## 공식 문서
- [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/checkpoints/)
- [Flink Configuration](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/config/)
- [Spark Configuration](https://spark.apache.org/docs/3.4.4/configuration.html)
