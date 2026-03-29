# [Plan] dbt Lineage View (리니지 뷰) 고도화

## 1. 개요
- **목적**: `dbt_unified_app.py` 내의 리니지 시각화 기능을 강화하여 모델 간의 의존 관계를 보다 명확하고 직관적으로 파악할 수 있도록 함.
- **대상**: 데이터 엔지니어 및 분석가
- **주요 가치**: 복잡한 모델 의존성을 한눈에 파악하고, 실행 상태 및 데이터 규모(Row Count)를 시각적으로 확인.

## 2. 문제 정의 (AS-IS)
- 현재 리니지는 버튼 그리드 형태로 나열되어 있어, 특정 모델이 어느 모델로부터 직접적으로 파생되었는지 알기 어려움 (단순 Depth 기반 나열).
- 모델의 최신 실행 상태나 데이터 규모를 리니지 뷰에서 즉시 확인하기 어려움.
- 대규모 프로젝트에서 특정 경로(Critical Path)를 추적하기 불편함.

## 3. 해결 방안 (TO-BE)
### 3.1. 의존성 그래프 고도화 (NetworkX 활용)
- `manifest.json`을 파싱하여 `NetworkX`를 이용한 DAG(Directed Acyclic Graph) 구조 구축.
- 현재의 버튼 그리드 방식을 유지하되, **직계 조상/자손(Direct Parents/Children) 강조** 기능 추가.
- 모델 클릭 시 해당 모델과 연결된 모든 에지(Edge)를 강조하여 경로 가시성 확보.

### 3.2. 실행 상태 및 메타데이터 통합
- `run_results.json` 데이터를 연동하여 노드(버튼)의 색상을 상태별로 변경 (성공: 녹색, 실패: 적색, 미실행: 회색).
- 노드 하단 또는 툴팁에 최신 실행 시의 `Rows Affected` 표시.
- 모델 타입(Incremental, View, Table)에 따른 아이콘 구분.

### 3.3. 상세 정보 사이드바 (Side Panel)
- 리니지에서 모델 선택 시, 우측 사이드바에 해당 모델의 상세 정보 표시:
    - 모델 설명 (Description)
    - 컬럼 리스트 및 타입
    - 컴파일된 SQL 뷰어
    - 최근 실행 이력 요약

### 3.4. 탐색 및 필터링 기능
- **Search**: 모델명 검색을 통한 리니지 즉시 이동.
- **Filter**: 특정 스키마/그룹만 리니지에 표시.
- **Depth Control**: 현재 구현된 Depth 조절 기능을 슬라이더나 보다 세밀한 컨트롤러로 개선.

## 4. 구현 단계
1. **[Phase 1] 데이터 엔진 강화**: `manifest_utils.py`에 NetworkX 기반의 그래프 분석 로직 추가 (직계 부모/자식 탐색).
2. **[Phase 2] UI/UX 개선**: `dbt_unified_app.py`의 리니지 렌더링 로직 수정 (강조 효과 및 상태 색상 적용).
3. **[Phase 3] 상세 정보 연동**: 사이드바 레이아웃 추가 및 모델 메타데이터 바인딩.
4. **[Phase 4] 검증 및 최적화**: 대규모 노드 노출 시 성능 최적화 및 사용자 피드백 반영.

## 5. 기대 효과
- 모델 영향도 분석(Impact Analysis) 시간 단축.
- 장애 발생 시 원인 모델(Upstream)과 영향 범위(Downstream)를 즉시 식별.
- 프로젝트 전체 구조에 대한 이해도 향상.

---
bkit Feature Usage
Used: PDCA Plan
Not Used: Graphviz (Not installed)
Recommended: /pdca design dbt_lineage_view
