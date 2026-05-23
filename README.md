# YBIGTA Spark & Flink 과제

Flink(스트리밍)와 Spark(배치)로 같은 데이터를 처리할 때 어떤 차이가 있는지 직접 확인해보는 과제입니다.

```
Flink  Kafka → 이벤트 도착 즉시 처리 → 결과
Spark  CSV 전체 로드 → 한번에 집계 → 결과
```

---

## 시작하기

```bash
# 1. 설정 파일 수정 (student/README.md 참고)
vim student/conf/flink-conf.yaml
vim student/conf/spark-defaults.conf

# 2. 이미지 빌드 (최초 1회)
./scripts/cluster.sh build

# 3. 클러스터 시작
./scripts/cluster.sh init

# 4. 실행
./run_all.sh

# 5. 결과 확인
cat results/result.json
```

**준비물**
- Docker Desktop — RAM 6GB 이상 할당 (Settings → Resources → Memory)
- Docker Compose v2
- 디스크 10GB 이상
- Python 3 (호스트에서 timestamp 계산에 사용)

**플랫폼별 안내**
- macOS / Linux: 그대로 실행 가능
- Windows: **WSL2** 안에서 진행하세요. Docker Desktop의 WSL2 백엔드를 켜고, WSL 터미널에서 클론·실행합니다.
- macOS에서 `timeout` 명령이 없다는 경고가 보이면: `brew install coreutils` (없어도 동작은 합니다)

---

## 작업 범위

`student/` 안에서만 수정하시면 됩니다.

| 파일 | 내용 |
|------|------|
| `student/conf/flink-conf.yaml` | Flink 체크포인팅 / 병렬성 |
| `student/conf/spark-defaults.conf` | Spark shuffle 파티션 수 |
| `student/REPORT.md` | 시나리오 01 · 03 · 04 관찰 기록 |

아래 파일들은 수정하지 마세요: `docker-compose.yml`, `jobs/`, `scenarios/`, `scripts/`

설정 방법은 [`student/README.md`](student/README.md)를 참고하세요.

---

## 시나리오

| # | 이름 | 내용 |
|---|------|------|
| 01 | `flink-basic` | 체크포인팅 설정 후 Flink 잡 실행. 설정이 빠지면 잡이 올라오지 않습니다. |
| 02 | `spark-batch` | CSV 읽고 groupBy 집계. `shuffle_partitions_used`가 설정한 값과 일치하는지 확인하세요. |
| 03 | `stream-vs-batch` | 같은 데이터를 두 엔진으로 처리하고 `latency_ratio`를 비교합니다. 왜 차이가 나는지 생각해보세요. |
| 04 | `spark-shuffle` | 기본값 200으로 먼저 돌려보고, 왜 느린지 확인한 뒤 적절한 값으로 조정합니다. |
| 05 | `pipeline` | Kafka → Flink → Spark 전체 흐름. flink/spark sha256이 모두 기댓값과 일치해야 합니다. |

5개 시나리오를 모두 통과하면 됩니다.

---

## 제출

```bash
./scripts/prepare_submission.sh <이름>
git commit -m "submit: <이름>"
git push
```

`submissions/<이름>/` 안에 아래 파일들만 포함되어야 합니다: `result.json`, `conf/`, `REPORT.md`

---

## 클러스터 관리

```bash
./scripts/cluster.sh build    # 이미지 빌드
./scripts/cluster.sh init     # 클러스터 시작
./scripts/cluster.sh status   # 상태 확인
./scripts/cluster.sh down     # 중지
./scripts/cluster.sh clean    # 초기화

# 브라우저에서 접속:
#   Flink UI  → http://localhost:8081
#   Spark UI  → http://localhost:8080
docker logs -f flink-jm      # Flink 로그
```
