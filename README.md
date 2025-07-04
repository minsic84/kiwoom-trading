# 🚀 주식 트레이딩 시스템

키움증권 OpenAPI를 활용한 주식 데이터 자동 수집 및 저장 시스템

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![PyQt5](https://img.shields.io/badge/PyQt5-5.15+-green.svg)
![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey.svg)

## 📖 프로젝트 개요

이 프로젝트는 **키움증권 OpenAPI**를 사용하여 한국 주식 시장의 일봉 데이터를 자동으로 수집하고 SQLite 데이터베이스에 저장하는 시스템입니다. 현재 **삼성전자(005930) 600개 데이터 수집 성공** 확인!

### ✨ 주요 성과
- ✅ 키움 OpenAPI 연결 및 로그인 자동화
- ✅ 일봉 데이터 600개 성공적 수집 (삼성전자)
- ✅ SQLite 데이터베이스 자동 저장
- ✅ 중복 데이터 방지 및 오류 처리
- ✅ GitHub 버전 관리 시스템 구축

## 🏗️ 시스템 아키텍처

```
📦 stock-trading-system/
├── 🔌 src/api/
│   ├── connector.py           # 키움 API 연결 관리
│   └── collectors/
│       └── daily_price.py     # 일봉 데이터 수집기 ⭐
├── 🗄️ src/core/
│   ├── config.py              # 시스템 설정
│   └── database.py            # DB 연결 및 관리
├── 🧪 scripts/
│   ├── test_daily_collector.py # 테스트 스크립트
│   ├── create_tables.py       # DB 테이블 생성
│   └── check_db.py            # DB 데이터 확인
├── 📋 requirements.txt
├── 🚫 .gitignore
└── 📖 README.md
```

## 🛠️ 설치 및 설정

### 1️⃣ 시스템 요구사항
- **OS**: Windows 10/11 (키움 OpenAPI 요구사항)
- **Python**: 3.8 이상
- **키움증권 계좌**: OpenAPI 사용 신청 필요

### 2️⃣ 프로젝트 클론
```bash
git clone https://github.com/minsic84/stock-trading-system.git
cd stock-trading-system
```

### 3️⃣ 가상환경 생성 및 활성화
```bash
# 32비트 가상환경 (키움 API 호환)
python -m venv venv-32bit
venv-32bit\Scripts\activate
```

### 4️⃣ 패키지 설치
```bash
pip install -r requirements.txt
```

### 5️⃣ 키움증권 OpenAPI 설치
1. [키움증권 OpenAPI 다운로드](https://www3.kiwoom.com/nkw.templateFrameSet.do?m=m1408000000)
2. 설치 후 계좌 개설 및 API 사용 신청
3. 실서버 또는 모의투자 접속 가능

### 6️⃣ 데이터베이스 초기화
```bash
python scripts/create_tables.py
```

## 🚀 사용법

### 📊 일봉 데이터 수집 테스트
```bash
python scripts/test_daily_collector.py
```

**실행 과정:**
1. 🔧 데이터베이스 연결 확인
2. 🔌 키움 API 로그인 (팝업창 나타남)
3. 📈 삼성전자(005930) 일봉 데이터 수집
4. 💾 SQLite DB에 자동 저장

### 📋 데이터 확인
```bash
python scripts/check_db.py
```

### 🔧 개별 종목 수집 (Python 코드)
```python
from src.api.collectors.daily_price import collect_daily_price_single

# 특정 종목 일봉 데이터 수집
success = collect_daily_price_single("005930")  # 삼성전자
if success:
    print("✅ 데이터 수집 성공!")
```

## 🗄️ 데이터베이스 구조

### 📊 daily_prices 테이블
| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| `id` | INTEGER | 기본키 | 1 |
| `stock_code` | TEXT | 종목코드 | "005930" |
| `date` | TEXT | 날짜 | "20250701" |
| `open_price` | INTEGER | 시가 | 60400 |
| `high_price` | INTEGER | 고가 | 61100 |
| `low_price` | INTEGER | 저가 | 60200 |
| `close_price` | INTEGER | 종가 | 60200 |
| `volume` | INTEGER | 거래량 | 13650991 |
| `trading_value` | INTEGER | 거래대금 | 827469 |
| `created_at` | TIMESTAMP | 생성시간 | 2025-07-03 12:00:00 |

### 📈 수집된 데이터 예시 (삼성전자)
```
날짜      | 종가     | 거래량       | 거래대금
20250701 | 60,200원 | 13,650,991주 | 827,469백만원
20250630 | 59,800원 | 17,110,294주 | 1,030,779백만원
20250627 | 60,800원 | 17,340,470주 | 1,053,939백만원
...
```

## ⚡ 주요 기능

### 🔌 API 연결 관리
- ✅ 자동 로그인 및 재연결
- ✅ API 요청 제한 관리 (딜레이 적용)
- ✅ 오류 복구 및 재시도 로직

### 📊 데이터 수집
- ✅ **TR 코드 opt10081** 사용 (일봉차트조회)
- ✅ **600개 데이터 동시 수집** 가능
- ✅ 실시간 진행 상황 표시
- ✅ 중복 데이터 자동 필터링

### 💾 데이터 저장
- ✅ SQLite 데이터베이스 자동 저장
- ✅ 테이블 자동 생성 및 인덱싱
- ✅ 데이터 무결성 보장

### 🛡️ 오류 처리
- ✅ 상세한 로깅 시스템
- ✅ API 연결 오류 복구
- ✅ 데이터 파싱 오류 처리

## 🔧 설정 옵션

`src/core/config.py`에서 다음 설정을 조정할 수 있습니다:

```python
class Config:
    # API 요청 딜레이 (ms)
    api_request_delay_ms = 1000
    
    # 데이터베이스 경로
    database_path = "stock_data.db"
    
    # 로그 레벨
    log_level = "INFO"
```

## 🚨 주의사항

### ⚠️ 키움증권 API 제한
- **운영시간**: 평일 08:30 ~ 15:30 (장중), 18:00 ~ 02:00 (야간)
- **요청 제한**: 초당 5회, 분당 100회 제한
- **동시 접속**: 1개 계좌당 1개 프로그램만 접속 가능

### 🖥️ 시스템 요구사항
- **Windows 환경 필수**: 키움 OpenAPI는 Windows에서만 작동
- **32비트 Python 권장**: 일부 기능 호환성 문제 방지
- **충분한 메모리**: 대량 데이터 수집 시 2GB 이상 권장

### 🔐 보안 주의사항
- API 키나 계좌 정보를 코드에 하드코딩하지 마세요
- `.gitignore`에 민감한 설정 파일 추가
- 실거래 계좌 사용 시 각별한 주의 필요

## 🎯 개발 로드맵

### ✅ 완료된 기능
- [x] 키움 OpenAPI 연결
- [x] 일봉 데이터 수집
- [x] SQLite 데이터베이스 저장
- [x] 기본 오류 처리
- [x] GitHub 버전 관리

### 🔄 진행 중인 기능
- [ ] 분봉 데이터 수집
- [ ] 다중 종목 배치 수집
- [ ] 웹 대시보드 개발

### 🚀 향후 계획
- [ ] 실시간 데이터 수집
- [ ] 기술적 분석 지표 계산
- [ ] 자동 매매 시스템 개발
- [ ] REST API 서버 구축

## 🤝 기여하기

1. 이 저장소를 Fork 합니다
2. 새로운 기능 브랜치를 생성합니다 (`git checkout -b feature/amazing-feature`)
3. 변경사항을 커밋합니다 (`git commit -m 'Add amazing feature'`)
4. 브랜치에 Push합니다 (`git push origin feature/amazing-feature`)
5. Pull Request를 생성합니다

## 📄 라이센스

이 프로젝트는 개인 학습 및 연구 목적으로 제작되었습니다. 상업적 이용 시 관련 법규를 확인하시기 바랍니다.

## 🆘 문제 해결

### 자주 묻는 질문

**Q: "키움 API가 연결되지 않아요"**
- A: Windows 환경인지 확인하고, 키움증권 OpenAPI가 설치되었는지 확인하세요.

**Q: "데이터가 저장되지 않아요"**
- A: `python scripts/create_tables.py`로 테이블을 먼저 생성하세요.

**Q: "600개 데이터가 모두 수집되지 않아요"**
- A: 키움 API 요청 제한을 확인하고, 네트워크 상태를 점검하세요.


### 🐛 버그 신고
이슈가 발생하면 [GitHub Issues](https://github.com/minsic84/stock-trading-system/issues)에 신고해주세요.

## 📞 연락처

프로젝트 관련 문의나 협업 제안은 GitHub Issues를 통해 연락해주세요.

---

⭐ **이 프로젝트가 도움이 되었다면 Star를 눌러주세요!** ⭐

🚀 **Happy Trading!** 📈

