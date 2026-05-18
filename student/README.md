# 수정 가능한 파일

| 파일 | 내용 |
|------|------|
| `conf/flink-conf.yaml` | Flink 체크포인팅 / 병렬성 |
| `conf/spark-defaults.conf` | Spark shuffle 파티션 수 |
| `docker-compose.student.yml` | 컨테이너 메모리 리소스 (선택) |

건드리면 안 되는 것: `../docker-compose.yml`, `../jobs/`, `../scenarios/`, `../scripts/`

---

## flink-conf.yaml

`# TODO` 블록의 설정 키들을 채운다. 각 키에 대한 설명은 주석 참고.

- [Flink Configuration 공식 문서](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/config/)
- [Checkpointing 공식 문서](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/checkpoints/)

---

## spark-defaults.conf

`spark.sql.shuffle.partitions`가 **200**으로 설정되어 있다. 이 상태로 실행하면 시나리오 04가 실패한다. 왜 실패하는지 이해하고 적절한 값으로 변경하여 통과시킨다.

- [Spark Configuration 공식 문서](https://spark.apache.org/docs/3.4.4/configuration.html)
