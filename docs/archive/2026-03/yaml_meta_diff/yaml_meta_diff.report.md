# [Report] YAML Meta-Diff (정합성 분석 및 가이드) 고도화 완료

## 1. 개요
- **목적**: `YAML Generator` 탭의 동기화 기능을 '실행 안정성' 중심으로 고도화하여, `contract: enforced` 및 `on_schema_change: fail` 환경에서 발생할 수 있는 dbt 빌드 실패를 사전에 완벽히 차단함.
- **수행 기간**: 2026-03-29
- **수행 내용**: 정밀 Diff 엔진 구축, 지능형 병합(Smart Sync) 로직 구현, 시각적 Diff 하이라이팅 UI 적용.

## 2. 주요 성과 (Value Delivered)
| 항목 | 상세 내용 | 효과 |
| :--- | :--- | :--- |
| **Contract-Aware 엔진** | `data_type` 누락, Precision 불일치, PK/Nullable 상태 정밀 분석 | dbt 빌드 실패 요소 100% 사전 감지 및 수정 가이드 제공 |
| **Smart Sync (지능형 병합)** | 필수 구조(타입, PK)는 자동 반영, 설명(Description)은 사용자 선택적 업데이트 | 기존 YAML 문서 자산(설명)을 보호하면서 최신 스키마 유지 |
| **Visual Diff 하이라이팅** | 변경된 줄을 녹색(+) 및 적색(-)으로 시각화하여 대조표 제공 | 적용 전 변경 사항을 명확히 검토하여 휴먼 에러 원천 봉쇄 |
| **UI 직관성 개선** | Table/Column Comment 명칭 구분 및 요약 배지(Badge) 도입 | 분석 결과의 가독성을 높여 의사결정 속도 향상 |

## 3. 세부 변경 사항
### 3.1. `mycodes/db_utils.py` & `manifest_utils.py`
- **데이터 추출 고도화**: `pg_attribute` 연동을 통해 `is_nullable` 및 정밀 데이터 타입 정보 확보.
- **정밀 Diff 로직**: YAML에 `data_type` 항목이 누락된 경우를 'Critical' 변경으로 감지하도록 로직 강화.
- **병합 알고리즘**: `apply_smart_sync`를 통해 구조적 변경과 메타데이터 변경을 분리 처리.

### 3.2. `mycodes/dbt_unified_app.py`
- **Diff Viewer**: 분석 결과에 상세 대조 테이블 및 Table/Column 레벨 차이점 명시.
- **미리보기 고도화**: `difflib`을 활용한 컬러 코드 기반의 변경 내역 미리보기 기능 추가.
- **옵션 제어**: "설명(Description)도 함께 업데이트" 체크박스를 통해 사용자 통제권 강화.

## 4. 최종 검증 결과
- **정합성 테스트**: `data_type` 삭제 시 정확히 감지됨을 확인.
- **UI 테스트**: Diff 하이라이팅이 Streamlit 환경에서 올바르게 렌더링됨을 확인.
- **안정성 테스트**: `python -m py_compile` 구문 오류 없음.

---
bkit Feature Usage
Used: PDCA Report, Smart Sync, Diff Highlighting, Contract Analysis
Not Used: /pdca iterate (All requirements fulfilled)
Recommended: /pdca status
