"""
파일 경로: src/core/data_validator.py

데이터 품질 검증 시스템
1. 누락된 거래일 체크
2. 가격 데이터 이상값 체크
3. 거래량 0인 데이터 체크
4. 중복 날짜 데이터 체크
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass

from .config import Config
from .database import get_database_service
from sqlalchemy import text

# 로거 설정
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """검증 결과 데이터 클래스"""
    stock_code: str
    check_type: str
    status: str  # 'PASS', 'WARNING', 'ERROR'
    message: str
    details: Dict[str, Any] = None


class TradingDateCalculator:
    """거래일 계산기 (한국 주식시장)"""

    @staticmethod
    def get_korean_holidays(year: int) -> List[date]:
        """한국 주요 공휴일 반환"""
        holidays = [
            date(year, 1, 1),   # 신정
            date(year, 3, 1),   # 삼일절
            date(year, 5, 5),   # 어린이날
            date(year, 6, 6),   # 현충일
            date(year, 8, 15),  # 광복절
            date(year, 10, 3),  # 개천절
            date(year, 10, 9),  # 한글날
            date(year, 12, 25), # 성탄절
        ]

        # 2025년 추가 공휴일
        if year == 2025:
            holidays.extend([
                date(2025, 1, 28),  # 설날 연휴
                date(2025, 1, 29),  # 설날
                date(2025, 1, 30),  # 설날 연휴
                date(2025, 5, 6),   # 어린이날 대체
                date(2025, 10, 6),  # 개천절 대체
            ])

        return holidays

    @staticmethod
    def is_trading_day(target_date: date) -> bool:
        """해당 날짜가 거래일인지 확인"""
        # 주말 체크
        if target_date.weekday() >= 5:  # 토(5), 일(6)
            return False

        # 공휴일 체크
        holidays = TradingDateCalculator.get_korean_holidays(target_date.year)
        if target_date in holidays:
            return False

        return True

    @staticmethod
    def get_trading_days_between(start_date: date, end_date: date) -> List[date]:
        """두 날짜 사이의 모든 거래일 반환"""
        trading_days = []
        current_date = start_date

        while current_date <= end_date:
            if TradingDateCalculator.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)

        return trading_days

    @staticmethod
    def get_recent_trading_days(days_count: int = 10) -> List[str]:
        """최근 N개 거래일 반환 (YYYYMMDD 형식)"""
        today = date.today()
        trading_days = []

        current_date = today
        while len(trading_days) < days_count and current_date >= date(2020, 1, 1):
            if TradingDateCalculator.is_trading_day(current_date):
                trading_days.append(current_date.strftime('%Y%m%d'))
            current_date -= timedelta(days=1)

        return trading_days


class DataQualityValidator:
    """데이터 품질 검증 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.db_service = get_database_service()
        self.trading_calculator = TradingDateCalculator()

    def validate_stock_data(self, stock_code: str) -> List[ValidationResult]:
        """종목 데이터 전체 검증"""
        results = []

        try:
            # 1. 테이블 존재 여부 확인
            if not self.db_service.table_manager.check_stock_table_exists(stock_code):
                results.append(ValidationResult(
                    stock_code=stock_code,
                    check_type="TABLE_EXISTS",
                    status="ERROR",
                    message="종목 테이블이 존재하지 않음"
                ))
                return results

            # 2. 기본 데이터 검증
            results.extend(self._check_basic_data_quality(stock_code))

            # 3. 거래일 누락 검증
            results.extend(self._check_missing_trading_days(stock_code))

            # 4. 가격 데이터 이상값 검증
            results.extend(self._check_price_anomalies(stock_code))

            # 5. 거래량 데이터 검증
            results.extend(self._check_volume_data(stock_code))

            # 6. 중복 데이터 검증
            results.extend(self._check_duplicate_dates(stock_code))

        except Exception as e:
            logger.error(f"종목 {stock_code} 검증 중 오류: {e}")
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="VALIDATION_ERROR",
                status="ERROR",
                message=f"검증 중 오류 발생: {str(e)}"
            ))

        return results

    def _check_basic_data_quality(self, stock_code: str) -> List[ValidationResult]:
        """기본 데이터 품질 검증"""
        results = []
        table_name = self.db_service.table_manager.get_stock_table_name(stock_code)

        try:
            with self.db_service.db_manager.get_session() as session:
                # 전체 데이터 개수
                total_query = text(f"SELECT COUNT(*) FROM {table_name}")
                total_count = session.execute(total_query).fetchone()[0]

                if total_count == 0:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="DATA_COUNT",
                        status="WARNING",
                        message="데이터가 없음",
                        details={"total_count": 0}
                    ))
                    return results

                # NULL 값 검증
                null_checks = {
                    "current_price": "종가가 NULL인 데이터",
                    "volume": "거래량이 NULL인 데이터",
                    "start_price": "시가가 NULL인 데이터",
                    "high_price": "고가가 NULL인 데이터",
                    "low_price": "저가가 NULL인 데이터"
                }

                for field, description in null_checks.items():
                    null_query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {field} IS NULL")
                    null_count = session.execute(null_query).fetchone()[0]

                    if null_count > 0:
                        results.append(ValidationResult(
                            stock_code=stock_code,
                            check_type="NULL_DATA",
                            status="WARNING",
                            message=f"{description}: {null_count}개",
                            details={"field": field, "null_count": null_count, "total_count": total_count}
                        ))

                # 기본 통계
                results.append(ValidationResult(
                    stock_code=stock_code,
                    check_type="DATA_COUNT",
                    status="PASS",
                    message=f"총 {total_count}개 데이터 존재",
                    details={"total_count": total_count}
                ))

        except Exception as e:
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="BASIC_CHECK",
                status="ERROR",
                message=f"기본 검증 실패: {str(e)}"
            ))

        return results

    def _check_missing_trading_days(self, stock_code: str) -> List[ValidationResult]:
        """누락된 거래일 체크"""
        results = []
        table_name = self.db_service.table_manager.get_stock_table_name(stock_code)

        try:
            with self.db_service.db_manager.get_session() as session:
                # 데이터 날짜 범위 조회
                date_range_query = text(f"""
                    SELECT MIN(date) as first_date, MAX(date) as last_date, COUNT(*) as data_count
                    FROM {table_name}
                """)
                range_result = session.execute(date_range_query).fetchone()

                if not range_result or not range_result[0]:
                    return results

                first_date_str = range_result[0]
                last_date_str = range_result[1]
                actual_count = range_result[2]

                # 날짜 변환
                first_date = datetime.strptime(first_date_str, '%Y%m%d').date()
                last_date = datetime.strptime(last_date_str, '%Y%m%d').date()

                # 기간 내 거래일 계산
                expected_trading_days = self.trading_calculator.get_trading_days_between(first_date, last_date)
                expected_count = len(expected_trading_days)

                missing_count = expected_count - actual_count

                if missing_count > 0:
                    # 실제 누락된 날짜 찾기
                    existing_dates_query = text(f"SELECT date FROM {table_name} ORDER BY date")
                    existing_dates = [row[0] for row in session.execute(existing_dates_query).fetchall()]

                    expected_dates = [d.strftime('%Y%m%d') for d in expected_trading_days]
                    missing_dates = [d for d in expected_dates if d not in existing_dates]

                    status = "WARNING" if missing_count <= 5 else "ERROR"

                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="MISSING_TRADING_DAYS",
                        status=status,
                        message=f"누락된 거래일: {missing_count}개",
                        details={
                            "expected_count": expected_count,
                            "actual_count": actual_count,
                            "missing_count": missing_count,
                            "missing_dates": missing_dates[:10],  # 최대 10개까지만
                            "date_range": f"{first_date_str} ~ {last_date_str}"
                        }
                    ))
                else:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="MISSING_TRADING_DAYS",
                        status="PASS",
                        message=f"거래일 데이터 완전함 ({actual_count}개)",
                        details={"expected_count": expected_count, "actual_count": actual_count}
                    ))

        except Exception as e:
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="MISSING_TRADING_DAYS",
                status="ERROR",
                message=f"거래일 체크 실패: {str(e)}"
            ))

        return results

    def _check_price_anomalies(self, stock_code: str) -> List[ValidationResult]:
        """가격 데이터 이상값 체크"""
        results = []
        table_name = self.db_service.table_manager.get_stock_table_name(stock_code)

        try:
            with self.db_service.db_manager.get_session() as session:
                # 가격 통계 조회
                price_stats_query = text(f"""
                    SELECT 
                        AVG(current_price) as avg_price,
                        MIN(current_price) as min_price,
                        MAX(current_price) as max_price,
                        COUNT(*) as total_count
                    FROM {table_name}
                    WHERE current_price IS NOT NULL AND current_price > 0
                """)
                stats = session.execute(price_stats_query).fetchone()

                if not stats or stats[3] == 0:
                    return results

                avg_price = stats[0]
                min_price = stats[1]
                max_price = stats[2]

                # 이상값 기준 (평균의 50% 미만 또는 300% 초과)
                anomaly_threshold_low = avg_price * 0.5
                anomaly_threshold_high = avg_price * 3.0

                # 이상값 데이터 조회
                anomaly_query = text(f"""
                    SELECT date, current_price 
                    FROM {table_name}
                    WHERE current_price < :low_threshold OR current_price > :high_threshold
                    ORDER BY date DESC
                    LIMIT 10
                """)
                anomalies = session.execute(anomaly_query, {
                    "low_threshold": anomaly_threshold_low,
                    "high_threshold": anomaly_threshold_high
                }).fetchall()

                # 0원 데이터 체크
                zero_price_query = text(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE current_price = 0 OR current_price IS NULL
                """)
                zero_count = session.execute(zero_price_query).fetchone()[0]

                if anomalies:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="PRICE_ANOMALIES",
                        status="WARNING",
                        message=f"가격 이상값 {len(anomalies)}개 발견",
                        details={
                            "avg_price": int(avg_price),
                            "anomalies": [(row[0], row[1]) for row in anomalies],
                            "threshold_low": int(anomaly_threshold_low),
                            "threshold_high": int(anomaly_threshold_high)
                        }
                    ))

                if zero_count > 0:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="ZERO_PRICE",
                        status="ERROR",
                        message=f"0원 또는 NULL 가격 데이터: {zero_count}개",
                        details={"zero_count": zero_count}
                    ))

                if not anomalies and zero_count == 0:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="PRICE_QUALITY",
                        status="PASS",
                        message="가격 데이터 품질 양호",
                        details={
                            "avg_price": int(avg_price),
                            "min_price": min_price,
                            "max_price": max_price
                        }
                    ))

        except Exception as e:
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="PRICE_ANOMALIES",
                status="ERROR",
                message=f"가격 이상값 체크 실패: {str(e)}"
            ))

        return results

    def _check_volume_data(self, stock_code: str) -> List[ValidationResult]:
        """거래량 데이터 검증"""
        results = []
        table_name = self.db_service.table_manager.get_stock_table_name(stock_code)

        try:
            with self.db_service.db_manager.get_session() as session:
                # 거래량 0인 데이터 체크
                zero_volume_query = text(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE volume = 0 OR volume IS NULL
                """)
                zero_volume_count = session.execute(zero_volume_query).fetchone()[0]

                # 평균 거래량 조회
                avg_volume_query = text(f"""
                    SELECT AVG(volume) FROM {table_name} 
                    WHERE volume > 0
                """)
                avg_volume_result = session.execute(avg_volume_query).fetchone()
                avg_volume = avg_volume_result[0] if avg_volume_result and avg_volume_result[0] else 0

                # 총 데이터 수
                total_query = text(f"SELECT COUNT(*) FROM {table_name}")
                total_count = session.execute(total_query).fetchone()[0]

                if zero_volume_count > 0:
                    zero_ratio = (zero_volume_count / total_count) * 100
                    status = "WARNING" if zero_ratio < 10 else "ERROR"

                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="ZERO_VOLUME",
                        status=status,
                        message=f"거래량 0인 데이터: {zero_volume_count}개 ({zero_ratio:.1f}%)",
                        details={
                            "zero_count": zero_volume_count,
                            "total_count": total_count,
                            "zero_ratio": zero_ratio
                        }
                    ))
                else:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="VOLUME_QUALITY",
                        status="PASS",
                        message="거래량 데이터 양호",
                        details={"avg_volume": int(avg_volume) if avg_volume else 0}
                    ))

        except Exception as e:
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="VOLUME_CHECK",
                status="ERROR",
                message=f"거래량 체크 실패: {str(e)}"
            ))

        return results

    def _check_duplicate_dates(self, stock_code: str) -> List[ValidationResult]:
        """중복 날짜 데이터 체크"""
        results = []
        table_name = self.db_service.table_manager.get_stock_table_name(stock_code)

        try:
            with self.db_service.db_manager.get_session() as session:
                # 중복 날짜 조회
                duplicate_query = text(f"""
                    SELECT date, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY date
                    HAVING COUNT(*) > 1
                    ORDER BY date DESC
                """)
                duplicates = session.execute(duplicate_query).fetchall()

                if duplicates:
                    total_duplicates = sum([row[1] - 1 for row in duplicates])  # 중복된 개수만 계산

                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="DUPLICATE_DATES",
                        status="ERROR",
                        message=f"중복 날짜 데이터: {len(duplicates)}개 날짜, 총 {total_duplicates}개 중복",
                        details={
                            "duplicate_dates": [(row[0], row[1]) for row in duplicates[:10]],
                            "total_duplicate_records": total_duplicates
                        }
                    ))
                else:
                    results.append(ValidationResult(
                        stock_code=stock_code,
                        check_type="DUPLICATE_DATES",
                        status="PASS",
                        message="중복 날짜 없음"
                    ))

        except Exception as e:
            results.append(ValidationResult(
                stock_code=stock_code,
                check_type="DUPLICATE_CHECK",
                status="ERROR",
                message=f"중복 체크 실패: {str(e)}"
            ))

        return results

    def validate_all_stocks(self) -> Dict[str, List[ValidationResult]]:
        """모든 종목 데이터 검증"""
        try:
            # 모든 활성 종목 조회
            active_stocks = self.db_service.metadata_manager.get_all_active_stocks()

            if not active_stocks:
                logger.warning("검증할 활성 종목이 없음")
                return {}

            all_results = {}

            print(f"🔍 {len(active_stocks)}개 종목 데이터 검증 시작...")

            for i, stock_info in enumerate(active_stocks):
                stock_code = stock_info['code']
                stock_name = stock_info['name']

                print(f"📊 검증 중: {stock_code} ({stock_name}) [{i+1}/{len(active_stocks)}]")

                # 종목별 검증 실행
                validation_results = self.validate_stock_data(stock_code)
                all_results[stock_code] = validation_results

                # 간단한 결과 요약 출력
                error_count = len([r for r in validation_results if r.status == "ERROR"])
                warning_count = len([r for r in validation_results if r.status == "WARNING"])

                if error_count > 0:
                    print(f"   ❌ 오류 {error_count}개, 경고 {warning_count}개")
                elif warning_count > 0:
                    print(f"   ⚠️ 경고 {warning_count}개")
                else:
                    print(f"   ✅ 정상")

            print(f"\n🎉 전체 종목 검증 완료!")
            return all_results

        except Exception as e:
            logger.error(f"전체 종목 검증 실패: {e}")
            return {}

    def generate_validation_report(self, validation_results: Dict[str, List[ValidationResult]]) -> str:
        """검증 결과 리포트 생성"""
        try:
            if not validation_results:
                return "검증 결과가 없습니다."

            report_lines = []
            report_lines.append("=" * 80)
            report_lines.append("📊 데이터 품질 검증 리포트")
            report_lines.append("=" * 80)
            report_lines.append(f"검증 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"검증 종목 수: {len(validation_results)}개")
            report_lines.append("")

            # 전체 요약
            total_errors = 0
            total_warnings = 0
            total_pass = 0

            for stock_results in validation_results.values():
                for result in stock_results:
                    if result.status == "ERROR":
                        total_errors += 1
                    elif result.status == "WARNING":
                        total_warnings += 1
                    elif result.status == "PASS":
                        total_pass += 1

            report_lines.append("📋 전체 요약:")
            report_lines.append(f"   ✅ 정상: {total_pass}개")
            report_lines.append(f"   ⚠️ 경고: {total_warnings}개")
            report_lines.append(f"   ❌ 오류: {total_errors}개")
            report_lines.append("")

            # 종목별 상세 결과
            report_lines.append("📈 종목별 상세 결과:")
            report_lines.append("-" * 80)

            for stock_code, results in validation_results.items():
                # 종목 정보 조회
                stock_info = None
                try:
                    active_stocks = self.db_service.metadata_manager.get_all_active_stocks()
                    stock_info = next((s for s in active_stocks if s['code'] == stock_code), None)
                except:
                    pass

                stock_name = stock_info['name'] if stock_info else "알 수 없음"

                errors = [r for r in results if r.status == "ERROR"]
                warnings = [r for r in results if r.status == "WARNING"]

                status_icon = "❌" if errors else ("⚠️" if warnings else "✅")

                report_lines.append(f"{status_icon} {stock_code} ({stock_name})")

                # 오류 먼저 출력
                for result in errors:
                    report_lines.append(f"   ❌ {result.check_type}: {result.message}")

                # 경고 출력
                for result in warnings:
                    report_lines.append(f"   ⚠️ {result.check_type}: {result.message}")

                # 정상인 경우 간단히 표시
                if not errors and not warnings:
                    pass_count = len([r for r in results if r.status == "PASS"])
                    report_lines.append(f"   ✅ 모든 검증 통과 ({pass_count}개 항목)")

                report_lines.append("")

            # 권장사항
            if total_errors > 0 or total_warnings > 0:
                report_lines.append("💡 권장사항:")
                report_lines.append("-" * 40)

                if total_errors > 0:
                    report_lines.append("🔧 오류 해결 방법:")
                    report_lines.append("   - 중복 데이터: 데이터 정리 스크립트 실행")
                    report_lines.append("   - 0원 데이터: 해당 날짜 데이터 재수집")
                    report_lines.append("   - 누락 데이터: 키움 API로 누락 기간 재수집")
                    report_lines.append("")

                if total_warnings > 0:
                    report_lines.append("⚠️ 경고 확인 사항:")
                    report_lines.append("   - 거래량 0: 거래정지일 또는 공휴일 확인")
                    report_lines.append("   - 가격 이상값: 액면분할, 합병 등 기업행동 확인")
                    report_lines.append("")

            report_lines.append("=" * 80)

            return "\n".join(report_lines)

        except Exception as e:
            logger.error(f"리포트 생성 실패: {e}")
            return f"리포트 생성 중 오류 발생: {str(e)}"


class DataQualityManager:
    """데이터 품질 관리 매니저"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.validator = DataQualityValidator(config)

    def run_daily_validation(self) -> str:
        """일일 데이터 품질 검증 실행"""
        try:
            print("🔍 일일 데이터 품질 검증 시작...")

            # 전체 종목 검증
            validation_results = self.validator.validate_all_stocks()

            if not validation_results:
                return "검증할 데이터가 없습니다."

            # 리포트 생성
            report = self.validator.generate_validation_report(validation_results)

            # 리포트 파일 저장
            report_dir = Path("reports")
            report_dir.mkdir(exist_ok=True)

            today = datetime.now().strftime('%Y%m%d')
            report_file = report_dir / f"data_quality_report_{today}.txt"

            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)

            print(f"📊 검증 리포트 저장: {report_file}")

            return report

        except Exception as e:
            logger.error(f"일일 검증 실패: {e}")
            return f"일일 검증 중 오류 발생: {str(e)}"

    def quick_validation(self, stock_codes: List[str]) -> str:
        """빠른 검증 (특정 종목들만)"""
        try:
            validation_results = {}

            for stock_code in stock_codes:
                results = self.validator.validate_stock_data(stock_code)
                validation_results[stock_code] = results

            return self.validator.generate_validation_report(validation_results)

        except Exception as e:
            logger.error(f"빠른 검증 실패: {e}")
            return f"빠른 검증 중 오류 발생: {str(e)}"


# 편의 함수들
def validate_stock_data_quality(stock_code: str) -> List[ValidationResult]:
    """단일 종목 데이터 품질 검증"""
    validator = DataQualityValidator()
    return validator.validate_stock_data(stock_code)


def run_full_data_validation() -> str:
    """전체 데이터 품질 검증 실행"""
    manager = DataQualityManager()
    return manager.run_daily_validation()


def validate_major_stocks() -> str:
    """주요 종목 빠른 검증"""
    major_stocks = ["005930", "000660", "035420", "005380", "068270"]
    manager = DataQualityManager()
    return manager.quick_validation(major_stocks)