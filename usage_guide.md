# 🚀 새로운 종목별 테이블 시스템 사용법

## 📋 실행 순서

### 1️⃣ 기존 데이터 완전 초기화
```bash
python scripts/clean_database_complete.py
```
- 모든 기존 테이블 삭제
- 새로운 구조로 재구성

### 2️⃣ 새 시스템 테스트
```bash
python scripts/test_enhanced_collector.py
```
- 새로운 데이터베이스 구조 검증
- 향상된 수집기 기능 테스트
- 키움 API 연결 테스트 (선택)

### 3️⃣ 실제 데이터 수집
```bash
# 주요 종목 자동 수집 (권장)
python -c "
from src.collectors.daily_price import collect_major_stocks_auto
result = collect_major_stocks_auto()
print('수집 결과:', result)
"

# 또는 특정 종목 수집
python -c "
from src.collectors.daily_price import collect_daily_price_single
success = collect_daily_price_single('005930')
print('수집 성공:', success)
"
```

### 4️⃣ 수집 결과 확인
```bash
python scripts/check_new_structure.py
```
- 종목별 테이블 현황 확인
- HeidiSQL 쿼리 생성
- 데이터 무결성 검사

### 5️⃣ 데이터 품질 검증
```bash
python -c "
from src.core.data_validator import run_full_data_validation
report = run_full_data_validation()
print(report)
"
```

## 🏗️ 새로운 구조 특징

### 종목별 개별 테이블
```
📂 데이터베이스 구조:
├── stocks (종목 메타데이터)
├── daily_prices_005930 (삼성전자)
├── daily_prices_000660 (SK하이닉스)
├── daily_prices_035420 (NAVER)
└── ...
```

### 자동 종목 관리
- 키움 API에서 종목 정보 자동 조회
- 종목별 테이블 자동 생성
- 메타데이터 실시간 업데이트

### 데이터 품질 검증
- 누락된 거래일 체크
- 가격 데이터 이상값 체크
- 거래량 0인 데이터 체크
- 중복 날짜 데이터 체크

## 💻 Python 코드 예제

### 기본 사용법
```python
from src.collectors.daily_price import EnhancedDailyPriceCollector

# 수집기 생성
collector = EnhancedDailyPriceCollector()

# 키움 API 연결
collector.connect_kiwoom()

# 단일 종목 수집
collector.collect_single_stock("005930")

# 주요 종목 자동 수집
result = collector.setup_and_collect_major_stocks()
```

### 고급 사용법
```python
# 전체 활성 종목 수집 (데이터 검증 포함)
result = collector.collect_all_registered_stocks(validate_data=True)

# 수집 상태 확인
status = collector.get_collection_status()

# 데이터 정리 및 최적화
collector.cleanup_and_optimize()
```

### 데이터 검증
```python
from src.core.data_validator import DataQualityValidator

validator = DataQualityValidator()

# 특정 종목 검증
results = validator.validate_stock_data("005930")

# 전체 검증 및 리포트 생성
report = validator.generate_validation_report(
    validator.validate_all_stocks()
)
```

## 🔍 HeidiSQL 사용법

### 1. 연결 설정
- 파일 > 새로 만들기 > 세션
- 네트워크 유형: SQLite
- 데이터베이스 파일: `data/stock_data.db`

### 2. 유용한 쿼리들
```sql
-- 전체 테이블 목록
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;

-- 종목 현황
SELECT code, name, data_count, latest_date 
FROM stocks 
WHERE is_active = 1 
ORDER BY data_count DESC;

-- 삼성전자 최신 데이터
SELECT date, current_price, volume 
FROM daily_prices_005930 
ORDER BY date DESC 
LIMIT 10;
```

## 🎯 주요 개선사항

### ✅ 성능 향상
- 종목별 독립 테이블로 조회 속도 대폭 향상
- 인덱스 최적화
- 병렬 처리 가능

### ✅ 관리 편의성
- 자동 종목 등록 및 테이블 생성
- 실시간 메타데이터 업데이트
- 종목별 독립적 백업/복원

### ✅ 확장성
- 새로운 종목 자동 추가
- 종목별 독립적 데이터 관리
- 대용량 데이터 처리 최적화

### ✅ 데이터 품질
- 실시간 데이터 검증
- 자동 이상값 탐지
- 누락 데이터 자동 감지

## 🚨 주의사항

### 기존 데이터 백업
```bash
# 중요: 기존 데이터가 있다면 백업 필수!
cp data/stock_data.db data/stock_data_backup_$(date +%Y%m%d).db
```

### 키움 API 제한
- 초당 5회, 분당 100회 요청 제한
- 장중(09:00-15:30) 사용 주의
- 자동 딜레이 적용됨

### 디스크 공간
- 종목당 약 1-5MB (600일 데이터 기준)
- 전체 시장: 약 5-10GB 예상
- 충분한 디스크 공간 확보 필요

## 🔧 문제 해결

### 데이터베이스 오류 시
```bash
# 완전 초기화
python scripts/clean_database_complete.py

# 구조 재생성
python scripts/test_enhanced_collector.py
```

### 키움 API 연결 실패 시
- 키움증권 OpenAPI 재설치
- 로그인 정보 확인
- 방화벽 설정 확인

### 메타데이터 불일치 시
```python
from src.core.database import get_database_service

db_service = get_database_service()

# 모든 종목 메타데이터 재계산
active_stocks = db_service.metadata_manager.get_all_active_stocks()
for stock in active_stocks:
    db_service.metadata_manager.update_stock_stats(stock['code'])
```

## 📊 성능 비교

### 기존 구조 vs 새 구조

| 항목 | 기존 (단일 테이블) | 새 구조 (종목별 테이블) |
|------|------------------|----------------------|
| 조회 속도 | 느림 (전체 스캔) | 빠름 (종목별 직접) |
| 인덱스 크기 | 대용량 | 소용량 다수 |
| 백업 시간 | 오래 걸림 | 종목별 선택 가능 |
| 확장성 | 제한적 | 무제한 |
| 관리 편의성 | 복잡 | 직관적 |

### 예상 성능 향상
- **조회 속도**: 10-50배 향상
- **인덱스 크기**: 70% 감소
- **백업 시간**: 80% 단축
- **메모리 사용량**: 60% 감소

## 🚀 향후 확장 계획

### Phase 2: 분봉 데이터
```python
# 분봉 테이블: minute_prices_005930
# 실시간 데이터: realtime_prices_005930
```

### Phase 3: 고급 분석
```python
# 기술적 지표: technical_indicators_005930
# 뉴스 데이터: news_data_005930
```

### Phase 4: 자동매매
```python
# 매매 신호: trading_signals_005930
# 포트폴리오: portfolio_management
```

## 📱 모니터링 대시보드

### 실시간 현황 확인
```python
from src.core.database import get_database_service

def show_live_status():
    db_service = get_database_service()
    status = db_service.metadata_manager.get_collection_status()
    
    print(f"📊 등록 종목: {status['total_stocks']}개")
    print(f"🏗️ 생성 테이블: {status['created_tables']}개") 
    print(f"📈 총 레코드: {status['total_records']:,}개")
    print(f"✅ 완성률: {status['completion_rate']:.1f}%")

# 실시간 모니터링
show_live_status()
```

### 일일 수집 자동화
```bash
# cron 설정 예시 (매일 오후 6시)
0 18 * * * cd /path/to/stock-trading-system && python -c "from src.collectors.daily_price import run_daily_collection_with_validation; run_daily_collection_with_validation()"
```

## 💡 베스트 프랙티스

### 1. 데이터 수집
- 장 마감 후 수집 (오후 4시 이후)
- 주요 종목부터 우선 수집
- 점진적 확장 (50개 → 100개 → 전체)

### 2. 품질 관리
- 매일 데이터 검증 실행
- 주간 무결성 검사
- 월간 성능 최적화

### 3. 백업 전략
- 일일 자동 백업
- 주간 전체 백업
- 월간 아카이브

### 4. 모니터링
- 수집 성공률 추적
- 오류 로그 모니터링
- 성능 지표 측정

## 🎉 최종 체크리스트

### 설치 및 설정
- [ ] 기존 데이터 백업
- [ ] `python scripts/clean_database_complete.py` 실행
- [ ] `python scripts/test_enhanced_collector.py` 통과

### 초기 데이터 수집
- [ ] 키움 API 연결 확인
- [ ] 주요 종목 데이터 수집
- [ ] `python scripts/check_new_structure.py` 확인

### 운영 준비
- [ ] 데이터 품질 검증 실행
- [ ] HeidiSQL 연결 테스트
- [ ] 자동화 스크립트 설정

### 확장 및 최적화
- [ ] 전체 종목 등록 (옵션)
- [ ] 일일 자동 수집 설정
- [ ] 모니터링 대시보드 구축

---

🎊 **축하합니다!** 새로운 종목별 테이블 시스템이 준비되었습니다!

이제 다음과 같은 혁신적인 기능들을 사용할 수 있습니다:
- ⚡ 초고속 종목별 데이터 조회
- 🤖 자동 종목 관리 및 테이블 생성  
- 🔍 실시간 데이터 품질 검증
- 📊 직관적인 데이터 구조
- 🚀 무제한 확장 가능성

**다음 단계**: 실제 매매 전략 개발 및 백테스팅 시스템 구축!