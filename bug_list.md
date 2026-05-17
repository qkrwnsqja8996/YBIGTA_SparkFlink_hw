# Bug List — YBIGTA SPFL Homework

총 **7개** 버그 발견 및 수정. 전부 코드 오류라 설정 문제는 없었음.

---

## BUG-01 · Flink REST API 호스트 접근 불가

**파일:** `scripts/lib/common.sh`

**증상:** 모든 Flink 시나리오(01, 03, 05)에서 `wait_for_flink_job` 타임아웃 발생.  
`Timed out waiting for Flink job to start`

**원인:**
```bash
# Before
FLINK_REST_URL="${FLINK_REST_URL:-http://flink-jm:8081}"
```
`flink-jm`은 Docker 내부 네트워크 호스트명이라 호스트에서 curl로 접근 불가.  
run_all.sh는 호스트에서 실행되는데 API URL이 컨테이너 내부 주소를 가리키고 있었음.

**수정:**
```bash
# After
FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"
```

---

## BUG-02 · Kafka 이벤트 주입 코드 (here-doc + 파이프 조합)

**파일:** `scenarios/03-stream-vs-batch/run.sh`

**증상:** 시나리오 03 Flink 출력이 항상 비어있음 → 해시 불일치.

**원인:**
```bash
# Before — 완전히 망가진 코드
python3 - "$EVENT_COUNT" | \
compose exec -T kafka kafka-console-producer ... <<'PYEOF'
import sys
n = int(sys.argv[1])
for i in range(n):
    print(f"event_type_{i % 5}:{i}")
PYEOF
```
bash에서 `cmd1 | cmd2 <<'HEREDOC'`은 heredoc이 `cmd2`의 stdin에 연결된다.  
결과적으로 kafka-console-producer가 Python **소스코드 자체**를 메시지로 Kafka에 전송.  
python3는 아무 입력도 못 받고 종료. 이벤트 0개 주입.

**수정:**
```bash
# After — 다른 시나리오와 동일하게 inject_data.sh 사용
bash "${PROJECT_ROOT}/scripts/inject_data.sh" \
    --scenario "$SCENARIO_ID" \
    --count "$EVENT_COUNT" \
    --topic "$TOPIC" >/dev/null 2>&1
```

---

## BUG-03 · Flink Consumer가 이전 시나리오 메시지까지 읽음

**파일:** `jobs/flink_counter.py`

**증상:** 시나리오 05에서 처리 이벤트 수가 예상값(2000)을 초과 → 해시 불일치.

**원인:**
```python
# Before
kafka_consumer.set_start_from_earliest()
```
각 시나리오 시작 시 consumer group offset을 `--to-latest`로 리셋하는데,  
`set_start_from_earliest()`는 group offset을 **무시**하고 항상 처음부터 읽음.  
→ 이전 시나리오에서 주입된 메시지까지 읽어서 count가 틀어짐.

**수정:**
```python
# After
kafka_consumer.set_start_from_group_offsets()
# committed offset 있으면 그걸 사용, 없으면 auto.offset.reset 따름
```

---

## BUG-04 · kafka-clients JAR 이미지 빌드 시 누락

**파일:** `docker/Dockerfile.flink`

**증상:** Flink 잡 제출 시 즉시 실패.  
`java.lang.NoClassDefFoundError: org/apache/kafka/common/serialization/ByteArrayDeserializer`

**원인:**
```dockerfile
# Before — kafka-clients가 조건부로만 다운로드됨
RUN curl ... flink-sql-kafka.jar || \
    curl ... flink-connector-kafka.jar

RUN curl ... kafka-clients-3.2.3.jar  # 이 줄도 있었지만 빌드 캐시나 네트워크 오류로 누락됨
```
`flink-connector-kafka-1.17.2.jar`는 `kafka-clients`에 **의존**하지만 번들링하지 않음.  
빌드 과정에서 JAR 다운로드가 누락되어 실행 시 ClassNotFound 발생.

**수정:**
```dockerfile
# After — 독립적인 RUN으로 확실히 다운로드 + 성공 검증
RUN curl -fL ... -o /opt/flink/lib/kafka-clients-3.2.3.jar && \
    echo "kafka-clients JAR downloaded: $(ls -lh /opt/flink/lib/kafka-clients-3.2.3.jar)"
```
런타임 임시 해결: 실행 중인 컨테이너에 직접 curl로 JAR 다운로드.

---

## BUG-05 · 시나리오 03 Kafka 토픽 삭제 후 메타데이터 갱신 딜레이

**파일:** `scenarios/03-stream-vs-batch/run.sh`

**증상:** Flink 잡이 RUNNING 상태임에도 90초 동안 이벤트를 전혀 처리하지 못함.  
`Flink 처리 타임아웃`

**원인:**
```bash
# Before
kafka-topics --delete --topic events
sleep 2
kafka-topics --create --topic events --partitions 1
```
토픽을 삭제하고 재생성하면 Kafka 브로커 내부 메타데이터 갱신에 수십 초가 걸림.  
그 사이 FlinkKafkaConsumer가 토픽을 찾지 못하거나 재시도 루프에 빠짐.  
1000개 이벤트가 Kafka에 있어도 Flink가 consume 시작을 못 함.

**수정:**
```bash
# After — 토픽 삭제 없이 consumer group offset만 리셋
kafka-consumer-groups \
    --group flink-counter-group \
    --topic events \
    --reset-offsets --to-latest \
    --execute
```
토픽이 살아있으므로 메타데이터 갱신 없이 즉시 consume 시작. 처리 시간 90s → 8s.

---

## BUG-06 · 이전 Flink 잡 미취소로 Consumer Group 충돌

**파일:** `scenarios/03-stream-vs-batch/run.sh`, `scenarios/05-pipeline/run.sh`

**증상:** 시나리오 03 실패 후 05 실행 시 offset reset이 무효화됨 → 이벤트 처리 타임아웃.

**원인:**
시나리오 03에서 `wait_for_flink_job` 타임아웃 발생 시 `output_fail`을 호출해 즉시 종료.  
이때 cleanup trap의 `JOB_ID`가 비어있어 Flink 잡이 **취소되지 않고 계속 실행**.  
살아있는 Flink 잡이 `flink-counter-group`을 점유하므로,  
시나리오 05의 `kafka-consumer-groups --reset-offsets`가 **active group에 대해 실패**하고 무시됨.  
결과적으로 05의 Flink 잡이 잘못된 offset부터 읽음.

**수정:** 각 시나리오 시작 전 `flink_cancel_all` 호출로 선제적으로 정리.
```bash
# scenarios/03과 05 모두에 추가
flink_cancel_all >/dev/null 2>&1 || true
sleep 2
```

---

## BUG-07 · compose exec print() stdout이 JSON 결과에 섞임

**파일:** `scenarios/04-spark-shuffle/run.sh`, `scenarios/05-pipeline/run.sh`

**증상:** 시나리오 04, 05가 내부 로그에서 "PASSED"라고 출력하는데도 run_all.sh에서 FAILED 처리.

**원인:**
```bash
# Before
compose exec -T spark-master python3 -c "
...
print('데이터 생성 완료')   # stdout으로 나감!
" 2>/dev/null
```
`2>/dev/null`은 stderr만 억제. compose exec의 **stdout은 시나리오 스크립트의 stdout**으로 전달됨.  
run_all.sh는 시나리오 stdout 전체를 JSON 파일로 저장 후 파싱하는데,  
파일 맨 앞에 한글 문자열이 섞이면 `json.loads()` 실패 → `passed=false`.

실제 파일 내용:
```
데이터 생성 완료          ← JSON 파서가 여기서 터짐
잡 스크립트 생성 완료
{"id":"04","name":"spark-shuffle","passed":true,...}
```

**수정:**
```bash
# After — print를 stderr로, 그리고 stdout 자체도 억제
print('데이터 생성 완료', file=__import__('sys').stderr)
...
" >/dev/null 2>&1
```

---

## 요약

| # | 파일 | 버그 유형 | 영향 시나리오 |
|---|------|----------|--------------|
| 1 | `scripts/lib/common.sh` | 잘못된 URL (내부 호스트명) | 01, 03, 05 |
| 2 | `scenarios/03/run.sh` | heredoc + pipe 조합 오용 | 03 |
| 3 | `jobs/flink_counter.py` | Kafka offset 정책 오설정 | 05 |
| 4 | `docker/Dockerfile.flink` | JAR 빌드 누락 | 01, 03, 05 |
| 5 | `scenarios/03/run.sh` | Kafka 토픽 재생성 딜레이 | 03 |
| 6 | `scenarios/03, 05/run.sh` | Flink 잡 미취소 → Group 충돌 | 05 |
| 7 | `scenarios/04, 05/run.sh` | stdout 오염으로 JSON 파싱 실패 | 04, 05 |
