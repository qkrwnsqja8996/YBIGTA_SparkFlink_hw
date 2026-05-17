# 제출 폴더

이 폴더에 최종 결과 파일을 복사하여 제출하세요.

## 제출 파일

```bash
cp results/result.json submissions/
```

## 제출 전 확인사항

- `result.json`의 `scenarios_passed` 값 확인
- 최소 시나리오 01 (flink-basic), 04 (spark-basic)이 `"passed": true`인지 확인

## 제출 파일 구조

```
submissions/
└── result.json   ← run_all.sh 실행 후 results/result.json 복사
```
