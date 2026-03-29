# [Report] dbt Lineage View 고도화 완료 - v2 (분리형 아키텍처)

## 1. 개요
- **목적**: 리니지 분석 기능을 독립된 탭으로 분리하고, 사용자 인터랙션(중앙 노드 클릭 허용, 소스 노드 클릭 제한)을 개선함.
- **수행 기간**: 2026-03-29
- **수행 내용**: `dbt_unified_app.py` 탭 구조 재편 및 노드 인터랙션 로직 수정.

## 2. 주요 성과 (Value Delivered)
| 항목 | 상세 내용 | 효과 |
| :--- | :--- | :--- |
| **탭 분리 (독립 분석)** | `🚀 dbt Runner`와 `🧬 리니지 분석` 탭 분리 | 각 기능별 전문성 강화 및 UI 복잡도 감소 |
| **중앙 노드 활성화** | 포커스 모델(중앙) 클릭 시 상세 정보 즉시 표시 | 분석 단계 축소 및 사용자 편의성 향상 |
| **소스 노드 제한** | 소스 모델(Source) 클릭 비활성화 (`disabled=True`) | 불필요한 에러 발생 방지 및 시각적 구분 명확화 |
| **실행 설정 유지** | Runner 탭의 Up/Down Depth 설정을 시각화와 독립적으로 유지 | 명령어 생성의 안정성 및 명확성 확보 |

## 3. 세부 변경 사항
### 3.1. `mycodes/dbt_unified_app.py`
- `tab_lineage` 신규 추가 및 리니지 UI 이관.
- `lt_center_model` 버튼에 `select_detail_model` 콜백 연결하여 중앙 노드 상세 조회 가능하게 수정.
- 소스 노드(`is_src`)의 경우 `st.button`에 `disabled=True`를 적용하여 인터랙션 차단.
- `tab_runner` 내 리니지 시각화 제거 및 실행 관련 설정값만 남김.

## 4. 최종 확인
- `python -m py_compile`을 통해 구문 오류 없음을 확인.
- `st.session_state`를 통한 모델 선택 정보 공유 확인.

---
bkit Feature Usage
Used: PDCA Report (Update), Tab Separation, Interaction Rules
Not Used: /pdca iterate (Final goal achieved)
Recommended: /pdca status
