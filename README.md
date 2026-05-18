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

---

## 작업 범위

`student/` 안에서만 수정하시면 됩니다.

| 파일 | 내용 |
|------|------|
| `student/conf/flink-conf.yaml` | Flink 체크포인팅 / 병렬성 |
| `student/conf/spark-defaults.conf` | Spark shuffle 파티션 수 |

아래 파일들은 수정하지 마세요: `docker-compose.yml`, `jobs/`, `scenarios/`, `scripts/`

설정 방법은 [`student/README.md`](student/README.md)를 참고하세요.

---

## 시나리오

| # | 이름 | 내용 |
|---|------|------|
| 01 | `flink-basic` | Flink 실시간 처리 |
| 02 | `spark-batch` | Spark 배치 집계 |
| 03 | `stream-vs-batch` | 같은 데이터, 두 엔진 비교 (`latency_ratio`) |
| 04 | `spark-shuffle` | 파티션 수에 따른 속도 차이 |
| 05 | `pipeline` | Kafka → Flink → Spark 전체 흐름 |

5개 시나리오를 모두 통과하면 됩니다.

---

## 제출

```bash
./scripts/prepare_submission.sh <이름>
git commit -m "submit: <이름>"
git push
```

`submissions/<이름>/` 안에 `result.json`과 `conf/`만 포함되어야 합니다.

---

## 클러스터 관리

```bash
./scripts/cluster.sh build    # 이미지 빌드
./scripts/cluster.sh init     # 클러스터 시작
./scripts/cluster.sh status   # 상태 확인
./scripts/cluster.sh down     # 중지
./scripts/cluster.sh clean    # 초기화

open http://localhost:8081    # Flink UI
open http://localhost:8080    # Spark UI
docker logs -f flink-jm      # Flink 로그
```
