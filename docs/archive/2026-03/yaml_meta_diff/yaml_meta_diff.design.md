# [Design] YAML Meta-Diff (정합성 분석 및 가이드) 고도화

## 1. 개요
본 설계는 `YAML Generator`에서 DB의 최신 스키마와 기존 dbt YAML 파일 간의 정밀한 차이를 분석하고, 프로젝트의 엄격한 규칙(`contract`, `on_schema_change`)을 준수하며 동기화할 수 있는 기능을 정의한다.

## 2. 시스템 아키텍처
```text
[db_utils.py] (Metadata Extractor)
  - get_table_detail(): 정밀 데이터 타입, Nullable 정보 추가 추출
          |
          v
[manifest_utils.py] (Diff Engine)
  - calculate_meta_diff(): DB 추출 정보와 기존 YAML 객체 비교 (deepdiff 활용)
  - apply_smart_sync(): 지능적 병합 로직 (구조 우선, 설명 보호)
          |
          v
[dbt_unified_app.py] (UI)
  - Visual Diff Viewer: 변경 내역 요약 및 상세 대조표
  - Smart Sync Button: 일괄 동기화 실행
```

## 3. 상세 설계

### 3.1. 데이터 추출 고도화 (`db_utils.py`)
- **`get_table_detail` 기능 확장**:
    - 기존: `name`, `data_type`, `description`, `pk`
    - 추가: `is_nullable` (bool), `data_type` 정밀화 (예: `varchar(50)`, `numeric(10,2)` 등 `format_type` 결과 활용)

### 3.2. 정밀 Diff 분석 엔진 (`manifest_utils.py`)
- **`calculate_meta_diff(db_meta, yaml_meta)`**:
    - **Added/Removed Columns**: DB와 YAML 간의 컬럼 존재 여부 대조.
    - **Type Mismatches (Strict)**: `data_type` 불일치뿐만 아니라 YAML에 **항목이 누락된 경우**도 정합성 위반으로 감지 (Contract 준수).
    - **PK Mismatches**: `unique_key`와 실제 PK 일치 여부.
    - **Comment Distinction**: 
        - **Table Comment**: 테이블 레벨의 설명 차이 별도 관리.
        - **Column Comment**: 각 컬럼별 설명 차이 별도 관리.
    - **Summary Generation**: 변경 내역을 직관적인 텍스트 요약(예: "📏 타입 불일치 1", "🆕 컬럼 추가 2")으로 반환.

### 3.3. 지능형 병합 (Smart Sync) 로직
- **로직 우선순위**:
    1. **구조적 변경 (필수 반영)**: 추가된 컬럼, 타입 변경(누락 포함), PK 변경, Nullable 설정.
    2. **메타데이터 변경 (선택 반영)**: 
        - **기본 설정 (ON)**: DB의 `COMMENT ON` 내용을 YAML `description`에 반영.
        - **사용자 옵션 (OFF)**: 기존 YAML에 작성된 설명을 보호하고 구조적 변경만 적용.
        - **예외**: YAML 설명이 비어있는 경우에는 항상 DB 내용을 반영하여 보완.

### 3.4. UI 컴포넌트 (`dbt_unified_app.py`)
- **변경 요약 배지(Badge)**: 분석 결과 목록에서 요약 정보를 태그 형식으로 노출.
- **Diff 상세 테이블**: 모델별 Expander 내부에 "컬럼명, 상태, 기존(YAML), 최신(DB)" 대조 표 제공.
- **Visual Diff 하이라이팅**: `difflib`을 활용하여 최종 적용 전 **녹색(+) / 적색(-)** 컬러 코드가 적용된 YAML 변경 내역 미리보기 제공.
- **동기화 옵션**: "설명(Description)도 함께 업데이트" 체크박스를 통한 제어권 부여.

## 4. 데이터 구조 (Diff Result)
```json
{
  "model_name": "stg_customers",
  "has_critical_change": true,
  "diffs": {
    "columns": {
      "new_col": {"status": "added", "db_type": "int"},
      "old_col": {"status": "removed"},
      "email": {"status": "type_mismatch", "yaml_type": "text", "db_type": "varchar(255)"}
    },
    "pk_changed": true,
    "description_changed": false
  }
}
```

## 5. 기대 효과
- **빌드 안정성**: `contract` 에러로 인한 `dbt run` 실패를 100% 사전에 방지.
- **관리 자동화**: 스키마 변경에 따른 YAML 동기화 작업의 노가다(수동 작업) 제거.
- **데이터 보호**: 사용자가 정성껏 작성한 YAML 내 설명은 안전하게 보호하며 구조만 업데이트.

---
bkit Feature Usage
Used: PDCA Design, Smart Sync, deepdiff, db_utils extension
Not Used: /pdca do (Next phase)
Recommended: /pdca do yaml_meta_diff
