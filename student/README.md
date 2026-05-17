# 수정 가능한 파일

| 파일 | 역할 |
|------|------|
| `conf/flink-conf.yaml` | Flink 체크포인팅 / 병렬성 설정 |
| `conf/spark-defaults.conf` | Spark shuffle 파티션 수 조정 |
| `docker-compose.student.yml` | 컨테이너 메모리 리소스 조정 (선택) |

수정하면 안 되는 것: `../docker-compose.yml`, `../jobs/`, `../scenarios/`, `../scripts/`

---

## flink-conf.yaml 설정 항목

`flink-conf.yaml`의 `# TODO` 블록에 아래 항목을 채워야 합니다.

### 체크포인팅 (시나리오 01, 03, 05 필수)

| 설정 키 | 설명 |
|---------|------|
| `execution.checkpointing.interval` | 체크포인트 주기 (단위: ms) |
| `execution.checkpointing.mode` | 처리 시맨틱 (`EXACTLY_ONCE` 또는 `AT_LEAST_ONCE`) |
| `state.checkpoints.dir` | 체크포인트 저장 경로 |
| `state.backend` | 상태 저장 방식 |

### 병렬성 (시나리오 03 latency_ratio에 영향)

| 설정 키 | 설명 |
|---------|------|
| `taskmanager.numberOfTaskSlots` | TaskManager 1대당 슬롯 수 |
| `parallelism.default` | Flink 잡 기본 병렬성 |

참고: [Flink Configuration 공식 문서](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/config/)

---

## spark-defaults.conf 설정 항목

`spark-defaults.conf`의 `# TODO` 블록을 채워야 합니다.

| 설정 키 | 설명 |
|---------|------|
| `spark.sql.shuffle.partitions` | `groupBy` / `join` 시 사용할 shuffle 파티션 수 |
| `spark.default.parallelism` | RDD 기본 병렬성 |

> Spark 기본값(200)은 수백 대 클러스터 기준입니다.  
> 소규모 데이터에 200개 파티션을 만들면 빈 파티션이 대부분이라 오히려 느려집니다.  
> 시나리오 04에서 `speedup_factor`로 직접 확인해보세요.

참고: [Spark Configuration 공식 문서](https://spark.apache.org/docs/3.4.4/configuration.html)
