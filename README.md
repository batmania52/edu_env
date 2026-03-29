# 🎓 dbt-Airflow 통합 교육 환경 (edu_env)

이 프로젝트는 데이터 엔지니어링 실습을 위해 **dbt**와 **Airflow**를 통합한 Docker 기반의 교육 환경입니다. 실제 비즈니스 시나리오를 바탕으로 데이터를 가공하고 워크플로우를 자동화하는 과정을 실습할 수 있습니다.

---

## 🛠️ 필수 준비 사항 (Prerequisites)

실습을 시작하기 전, 아래 도구들이 반드시 설치되어 있어야 합니다.

1.  **Docker Desktop**: 데이터베이스(Postgres)와 Airflow를 컨테이너로 실행하기 위해 필요합니다.
2.  **Python 3.11**: dbt 실행 및 가상 환경(venv) 설정을 위해 필요합니다.
3.  **DB Tool (DBeaver 추천)**: 데이터베이스 내부의 테이블 구조와 데이터를 조회하기 위해 필요합니다.
4.  **Git**: 소스 코드 클론 및 버전 관리를 위해 필요합니다.

---

## 🚀 시작하기 (Getting Started)

### 1. 저장소 클론
터미널을 열고 다음 명령어를 실행하여 프로젝트를 클론합니다.

```bash
git clone https://github.com/batmania52/edu_env.git
cd edu_env
```

### 2. 상세 환경 설정
클론이 완료되었다면, 프로젝트 루트에 있는 **`STUDENT_GUIDE.md`** 파일을 열어 1단계부터 차근차근 따라하며 환경을 구축하세요.

---

## 🌐 서비스 접속 정보

| 서비스 | 접속 주소 | 로그인 계정 (ID / PW) |
| :--- | :--- | :--- |
| **Airflow Web UI** | [http://localhost:8081](http://localhost:8081) | `airflow` / `airflow` |
| **Postgres (외부 접속)** | `localhost:5433` | `airflow` / `airflow` (DB: `airflow`) |

---

## 📚 기술 스택
- **dbt-core**: 데이터 변환 및 모델링
- **Apache Airflow**: 워크플로우 오케스트레이션
- **Astronomer Cosmos**: dbt-Airflow 통합 플러그인
- **Postgres**: 데이터 저장 및 조회
- **Docker**: 인프라 컨테이너화

---
© 2026 dbt-Airflow Training Program. All rights reserved.
