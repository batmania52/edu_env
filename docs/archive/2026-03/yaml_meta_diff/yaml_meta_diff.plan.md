# [Plan] YAML Meta-Diff (정합성 분석 및 가이드) 고도화 - v2

## 1. 개요
- **목적**: `YAML Generator` 탭에서 DB 스키마와 기존 dbt YAML 간의 차이점을 상세히 분석하고, 프로젝트의 엄격한 규칙(`contract`, `on_schema_change`)에 따른 실행 실패를 사전에 방지함.
- **대상**: 데이터 엔지니어 및 분석가
- **주요 가치**: DB-YAML 간의 정합성 보장, 증분 모델 실행 실패(Build Failure) 사전 차단, 지능형 스키마 동기화(Smart Sync).

## 2. 문제 정의 (AS-IS)
- 현재는 단순히 "내용 동일/다름" 여부만 판단하여, 구체적으로 어떤 항목(컬럼, 타입, PK 등)이 다른지 알 수 없음.
- `contract: enforced` 설정 시 데이터 타입이 미세하게 다르면 실행이 실패하는데, 이를 사전에 감지하지 못함.
- `on_schema_change: fail` 설정으로 인해 DB에 신규 컬럼이 추가된 경우 YAML을 수동으로 일일이 업데이트해야 함.

## 3. 해결 방안 (TO-BE)
### 3.1. [Critical] 실행 정합성 정밀 분석 (Contract-Aware)
- **데이터 타입(Data Type) 검증**: DB의 실제 타입과 YAML의 `data_type`을 1:1 비교하여 불일치 시 경고 표시.
- **PK 및 unique_key 동기화**: DB의 Primary Key 리스트와 YAML의 `unique_key` 설정을 비교하여 불일치 시 증분 업데이트 위험 알림.
- **제약 조건(Constraints) 감지**: `NOT NULL` 등 DB 제약 조건과 YAML의 `constraints` 정합성 체크.

### 3.2. [High] 스키마 변경 이력 시각화 (Schema Drift Viewer)
- **Visual Diff**: 추가된 컬럼(🆕), 삭제된 컬럼(🗑️), 변경된 항목(📝)을 색상과 아이콘으로 구분하여 리스트업.
- **상세 내역 요약**: 모델별로 변경된 항목의 종류와 개수를 한눈에 파악할 수 있는 요약 정보 제공.

### 3.3. [Simple] 지능형 일괄 반영 (Smart Sync)
- **복잡한 선택 대신 자동 병합**: 개별 항목 선택 대신, `contract` 유지에 필수적인 구조적 변경(타입, PK)은 우선 반영하고, 메타데이터(설명)는 지능적으로 병합(Smart Merge)하여 일괄 업데이트.
- **설명(Description) 보호**: YAML에 이미 작성된 설명이 있는 경우, DB의 빈 코멘트로 덮어씌워지지 않도록 보호 로직 적용.

## 4. 구현 단계
1. **[Phase 1] 데이터 분석 엔진 강화**: `manifest_utils.py`의 `get_table_detail`을 확장하여 PK, Nullable, 정확한 데이터 타입을 추출하고 `deepdiff` 기반의 분석 로직 추가.
2. **[Phase 2] 상세 분석 UI 구현**: `dbt_unified_app.py`의 분석 결과 영역에 "변경 상세 보기(Diff View)" 버튼 및 확장 패널 추가.
3. **[Phase 3] 정합성 가이드 로직**: `contract` 및 `on_schema_change` 규칙에 따른 "실행 가능 여부" 예측 및 경고 메시지 출력.
4. **[Phase 4] Smart Sync 업데이트**: 지능형 병합 로직을 적용한 일괄 YAML 반영 기능 구현.

## 5. 기대 효과
- **무결성 향상**: `contract: enforced` 기반의 안정적인 dbt 모델 운영.
- **생산성 증대**: 스키마 변경 시 수동 YAML 수정 작업 시간 90% 이상 단축.
- **정확성**: DB와 YAML 간의 메타데이터 불일치로 인한 분석 혼선 방지.

---
bkit Feature Usage
Used: PDCA Plan (v2 Update), Smart Sync, Contract Analysis
Not Used: Partial Update (Removed for simplicity)
Recommended: /pdca design yaml_meta_diff
