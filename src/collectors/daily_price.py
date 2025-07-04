"""
파일 경로: src/collectors/daily_price.py

Enhanced Daily Price Collector
종목별 개별 테이블 구조 + 자동 종목 등록 + 데이터 품질 검증
"""
import logging
from typing import List, Dict, Any, Optional, Tuple, Callable
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import get_database_service
from ..core.stock_manager import create_stock_manager
from ..core.data_validator import DataQualityValidator
from ..api.connector import KiwoomAPIConnector, get_kiwoom_connector

# 로거 설정
logger = logging.getLogger(__name__)


class EnhancedDailyPriceCollector:
    """향상된 일봉 데이터 수집기"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.kiwoom = None
        self.db_service = get_database_service()
        self.stock_manager = create_stock_manager(config)
        self.data_validator = DataQualityValidator(config)

        # 수집 상태
        self.collected_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.registered_stocks = 0

        # TR 코드 정의
        self.TR_CODE = "opt10081"  # 일봉차트조회
        self.RQ_NAME = "일봉차트조회"

        logger.info("향상된 일봉 데이터 수집기 초기화 완료")

    def connect_kiwoom(self, auto_login: bool = True) -> bool:
        """키움 API 연결"""
        try:
            self.kiwoom = get_kiwoom_connector(self.config)

            if auto_login and not self.kiwoom.is_connected:
                logger.info("키움 API 로그인 시도...")
                if self.kiwoom.login():
                    logger.info("키움 API 로그인 성공")
                    return True
                else:
                    logger.error("키움 API 로그인 실패")
                    return False

            return True

        except Exception as e:
            logger.error(f"키움 API 연결 실패: {e}")
            return False

    def register_stock_if_needed(self, stock_code: str, stock_name: str = None) -> bool:
        """필요시 종목 등록 및 테이블 생성"""
        try:
            # 종목명이 없으면 키움 API에서 조회
            if not stock_name and self.kiwoom:
                stock_name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", stock_code)
                if stock_name:
                    stock_name = stock_name.strip()

            # 종목 등록 및 테이블 준비
            if self.db_service.prepare_stock_for_collection(stock_code, stock_name, "KOSPI"):  # 기본 KOSPI
                logger.info(f"종목 {stock_code} 수집 준비 완료")
                self.registered_stocks += 1
                return True
            else:
                logger.error(f"종목 {stock_code} 수집 준비 실패")
                return False

        except Exception as e:
            logger.error(f"종목 {stock_code} 등록 실패: {e}")
            return False

    def collect_single_stock(self, stock_code: str, start_date: str = None,
                             end_date: str = None, update_existing: bool = True) -> bool:
        """단일 종목 일봉 데이터 수집 (향상된 버전)"""
        try:
            print(f"\n{'='*20} {stock_code} 수집 시작 {'='*20}")

            if not self.kiwoom or not self.kiwoom.is_connected:
                print("❌ 키움 API가 연결되지 않음")
                logger.error("키움 API가 연결되지 않음")
                return False

            # 1. 종목 등록 및 테이블 준비
            print(f"🔧 종목 {stock_code} 수집 준비 중...")
            if not self.register_stock_if_needed(stock_code):
                self.error_count += 1
                return False

            # 2. 기존 데이터 확인
            latest_date = self.db_service.get_stock_latest_date(stock_code)
            print(f"📅 기존 최신 데이터: {latest_date if latest_date else '없음'}")

            if not update_existing and latest_date:
                if self._should_skip_update(latest_date):
                    print(f"⏭️ 최신 데이터 존재, 수집 건너뛰기")
                    self.skipped_count += 1
                    return True

            logger.info(f"종목 {stock_code} 일봉 데이터 수집 시작")

            # 3. TR 요청 데이터 준비
            input_data = {
                "종목코드": stock_code,
                "기준일자": end_date or "20250701",  # 최신 데이터부터
                "수정주가구분": "1"  # 수정주가 적용
            }

            print(f"📡 TR 요청 데이터: {input_data}")

            # 4. 데이터 수집 루프
            collected_data = []
            prev_next = "0"
            request_count = 0
            max_requests = 20  # 더 많은 데이터 수집 가능

            while request_count < max_requests:
                print(f"🔄 TR 요청 {request_count + 1}/{max_requests}")

                # TR 요청
                response = self.kiwoom.request_tr_data(
                    rq_name=self.RQ_NAME,
                    tr_code=self.TR_CODE,
                    input_data=input_data,
                    prev_next=prev_next
                )

                if not response:
                    print("❌ TR 요청 실패")
                    logger.error(f"{stock_code} TR 요청 실패")
                    self.error_count += 1
                    break

                # 5. 데이터 파싱
                daily_data = self._parse_daily_data(response, stock_code)
                if not daily_data:
                    print("⚠️ 파싱된 데이터 없음")
                    break

                print(f"📊 수집된 데이터: {len(daily_data)}개")
                collected_data.extend(daily_data)

                # 6. 연속 조회 확인
                prev_next = response.get('prev_next', '0')
                if prev_next != '2':
                    print("✅ 모든 데이터 수집 완료")
                    break

                request_count += 1
                # API 요청 제한 대기
                time.sleep(self.config.api_request_delay_ms / 1000)

            # 7. 데이터베이스 저장
            if collected_data:
                saved_count = self._save_daily_data_to_stock_table(stock_code, collected_data)
                print(f"💾 저장 완료: {saved_count}개")
                logger.info(f"{stock_code} 일봉 데이터 저장 완료: {saved_count}개")
                self.collected_count += saved_count

                # 8. 데이터 품질 검증 (옵션)
                if self.config.debug:
                    print("🔍 데이터 품질 검증 중...")
                    validation_results = self.data_validator.validate_stock_data(stock_code)
                    error_results = [r for r in validation_results if r.status == "ERROR"]
                    if error_results:
                        print(f"⚠️ 품질 검증 오류 {len(error_results)}개 발견")
                    else:
                        print("✅ 데이터 품질 검증 통과")

                return True
            else:
                print("❌ 수집된 데이터 없음")
                logger.warning(f"{stock_code} 수집된 데이터 없음")
                return False

        except Exception as e:
            print(f"💥 치명적 오류: {e}")
            logger.error(f"{stock_code} 일봉 데이터 수집 중 오류: {e}")
            self.error_count += 1
            return False

    def _parse_daily_data(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """일봉 데이터 파싱 (기존 로직 유지하되 로깅 개선)"""
        try:
            tr_code = response.get('tr_code')
            if tr_code != self.TR_CODE:
                logger.warning(f"예상하지 못한 TR 코드: {tr_code}")
                return []

            # connector에서 이미 파싱된 데이터 사용
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error(f"데이터가 파싱되지 않음: {data_info}")
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.warning("원시 데이터가 없음")
                return []

            daily_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    # 기본 필드 추출
                    date = row_data.get("일자", "").strip()
                    current_price = row_data.get("현재가", "").strip()
                    volume = row_data.get("거래량", "").strip()
                    trading_value = row_data.get("거래대금", "").strip()
                    start_price = row_data.get("시가", "").strip()
                    high_price = row_data.get("고가", "").strip()
                    low_price = row_data.get("저가", "").strip()

                    # 필수 데이터 확인
                    if not date or not current_price:
                        continue

                    # 숫자 변환 및 정제
                    try:
                        current_price_int = self._clean_and_convert_to_int(current_price)
                        volume_int = self._clean_and_convert_to_int(volume)
                        trading_value_int = self._clean_and_convert_to_int(trading_value)
                        start_price_int = self._clean_and_convert_to_int(start_price)
                        high_price_int = self._clean_and_convert_to_int(high_price)
                        low_price_int = self._clean_and_convert_to_int(low_price)

                        if current_price_int <= 0:
                            continue

                        data_item = {
                            'date': date,
                            'current_price': current_price_int,
                            'volume': volume_int,
                            'trading_value': trading_value_int,
                            'start_price': start_price_int,
                            'high_price': high_price_int,
                            'low_price': low_price_int,
                            'prev_day_diff': 0,  # 추후 계산
                            'change_rate': 0.0   # 추후 계산
                        }

                        daily_data.append(data_item)

                    except (ValueError, TypeError) as e:
                        logger.debug(f"데이터 변환 오류 (행 {i}): {e}")
                        continue

                except Exception as e:
                    logger.debug(f"행 처리 오류 {i}: {e}")
                    continue

            logger.info(f"파싱 완료: {len(daily_data)}개 데이터")
            return daily_data

        except Exception as e:
            logger.error(f"파싱 치명적 오류: {e}")
            return []

    def _clean_and_convert_to_int(self, value: str) -> int:
        """문자열을 정수로 안전하게 변환"""
        if not value:
            return 0

        # 부호, 콤마, 공백 제거
        cleaned = value.replace('+', '').replace('-', '').replace(',', '').strip()

        if not cleaned:
            return 0

        try:
            return int(cleaned)
        except (ValueError, TypeError):
            return 0

    def _save_daily_data_to_stock_table(self, stock_code: str, daily_data: List[Dict[str, Any]]) -> int:
        """종목별 테이블에 일봉 데이터 저장"""
        saved_count = 0

        try:
            for data in daily_data:
                success = self.db_service.add_daily_price_to_stock(
                    stock_code=stock_code,
                    date=data['date'],
                    current_price=data['current_price'],
                    volume=data['volume'],
                    trading_value=data['trading_value'],
                    start_price=data['start_price'],
                    high_price=data['high_price'],
                    low_price=data['low_price'],
                    prev_day_diff=data['prev_day_diff'],
                    change_rate=data['change_rate']
                )

                if success:
                    saved_count += 1
                else:
                    logger.warning(f"{stock_code} 데이터 저장 실패: {data['date']}")

        except Exception as e:
            logger.error(f"{stock_code} 데이터 저장 중 오류: {e}")

        return saved_count

    def _should_skip_update(self, latest_date: str) -> bool:
        """데이터 업데이트 건너뛸지 판단"""
        try:
            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
            today = datetime.now()

            # 최신 데이터가 오늘이면 건너뛰기
            if latest_dt.date() >= today.date():
                return True

            # 주말 고려한 판단 로직
            days_diff = (today.date() - latest_dt.date()).days

            if today.weekday() == 0:  # 월요일
                return days_diff <= 3  # 금요일 데이터까지 있으면 OK
            elif today.weekday() == 6:  # 일요일
                return days_diff <= 2  # 금요일 데이터까지 있으면 OK
            else:
                return days_diff <= 1  # 어제 데이터까지 있으면 OK

        except Exception as e:
            logger.error(f"업데이트 판단 오류: {e}")
            return False  # 오류 시 수집 수행

    def collect_multiple_stocks(self, stock_codes: List[str],
                              start_date: str = None, end_date: str = None,
                              update_existing: bool = True,
                              progress_callback: Optional[Callable] = None,
                              validate_data: bool = False) -> Dict[str, Any]:
        """다중 종목 일봉 데이터 수집 (향상된 버전)"""

        logger.info(f"다중 종목 일봉 데이터 수집 시작: {len(stock_codes)}개 종목")
        print(f"\n🚀 다중 종목 데이터 수집 시작")
        print(f"📊 대상 종목: {len(stock_codes)}개")
        print(f"🔄 업데이트 모드: {'ON' if update_existing else 'OFF'}")

        # 통계 초기화
        self.collected_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.registered_stocks = 0

        results = {
            'success': [],
            'failed': [],
            'skipped': [],
            'registered': [],
            'total_collected': 0,
            'total_errors': 0,
            'total_skipped': 0,
            'validation_results': {}
        }

        start_time = datetime.now()

        for idx, stock_code in enumerate(stock_codes):
            try:
                print(f"\n📈 진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")
                logger.info(f"진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")

                # 진행률 콜백 호출
                if progress_callback:
                    progress_callback(idx + 1, len(stock_codes), stock_code)

                # 종목별 데이터 수집
                success = self.collect_single_stock(
                    stock_code, start_date, end_date, update_existing
                )

                if success:
                    results['success'].append(stock_code)
                    print(f"✅ {stock_code} 수집 성공")
                else:
                    results['failed'].append(stock_code)
                    print(f"❌ {stock_code} 수집 실패")

                # 데이터 품질 검증 (옵션)
                if validate_data and success:
                    print(f"🔍 {stock_code} 데이터 품질 검증 중...")
                    validation_result = self.data_validator.validate_stock_data(stock_code)
                    results['validation_results'][stock_code] = validation_result

                # API 요청 제한 대기
                if idx < len(stock_codes) - 1:  # 마지막이 아닌 경우
                    delay = self.config.api_request_delay_ms / 1000
                    print(f"⏱️ API 제한 대기: {delay}초")
                    time.sleep(delay)

            except Exception as e:
                logger.error(f"{stock_code} 수집 중 예외 발생: {e}")
                results['failed'].append(stock_code)
                self.error_count += 1
                print(f"💥 {stock_code} 예외 발생: {e}")

        # 최종 통계
        results['total_collected'] = self.collected_count
        results['total_errors'] = self.error_count
        results['total_skipped'] = self.skipped_count
        results['total_registered'] = self.registered_stocks
        results['elapsed_time'] = (datetime.now() - start_time).total_seconds()

        # 결과 요약 출력
        print(f"\n🎉 다중 종목 수집 완료!")
        print(f"   ✅ 성공: {len(results['success'])}개")
        print(f"   ❌ 실패: {len(results['failed'])}개")
        print(f"   ⏭️ 건너뛰기: {len(results['skipped'])}개")
        print(f"   🆕 신규 등록: {results['total_registered']}개")
        print(f"   📊 총 수집 레코드: {results['total_collected']:,}개")
        print(f"   ⏱️ 소요 시간: {results['elapsed_time']:.1f}초")

        logger.info(f"다중 종목 수집 완료: 성공 {len(results['success'])}개, "
                   f"실패 {len(results['failed'])}개, 건너뛰기 {len(results['skipped'])}개")

        return results

    def collect_all_registered_stocks(self, progress_callback: Optional[Callable] = None,
                                    validate_data: bool = True) -> Dict[str, Any]:
        """등록된 모든 활성 종목 데이터 수집"""
        try:
            print("📋 등록된 활성 종목 조회 중...")

            # 활성 종목 조회
            active_stocks = self.db_service.metadata_manager.get_all_active_stocks()

            if not active_stocks:
                print("⚠️ 등록된 활성 종목이 없습니다.")
                logger.warning("등록된 활성 종목이 없음")
                return {'error': '등록된 활성 종목이 없음'}

            stock_codes = [stock['code'] for stock in active_stocks]

            print(f"📊 총 {len(stock_codes)}개 활성 종목 발견")

            # 다중 수집 실행
            return self.collect_multiple_stocks(
                stock_codes=stock_codes,
                update_existing=True,
                progress_callback=progress_callback,
                validate_data=validate_data
            )

        except Exception as e:
            logger.error(f"전체 종목 수집 실패: {e}")
            return {'error': f'전체 종목 수집 실패: {str(e)}'}

    def setup_and_collect_major_stocks(self) -> Dict[str, Any]:
        """주요 종목 자동 설정 및 수집"""
        try:
            print("🔧 주요 종목 자동 설정 중...")

            # 주요 종목 등록
            major_stock_codes = self.stock_manager.setup_major_stocks_for_testing()

            if not major_stock_codes:
                return {'error': '주요 종목 설정 실패'}

            print(f"✅ {len(major_stock_codes)}개 주요 종목 등록 완료")

            # 데이터 수집
            return self.collect_multiple_stocks(
                stock_codes=major_stock_codes,
                update_existing=True,
                validate_data=True
            )

        except Exception as e:
            logger.error(f"주요 종목 설정 및 수집 실패: {e}")
            return {'error': f'주요 종목 설정 및 수집 실패: {str(e)}'}

    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환 (향상된 버전)"""
        try:
            # 기본 상태
            basic_status = {
                'collected_count': self.collected_count,
                'error_count': self.error_count,
                'skipped_count': self.skipped_count,
                'registered_stocks': self.registered_stocks,
                'kiwoom_connected': self.kiwoom.is_connected if self.kiwoom else False,
                'db_connected': self.db_service is not None
            }

            # 데이터베이스 현황
            db_status = self.db_service.metadata_manager.get_collection_status()

            # 테이블 목록
            stock_tables = self.db_service.table_manager.get_all_stock_tables()

            return {
                **basic_status,
                **db_status,
                'stock_tables_count': len(stock_tables),
                'last_updated': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"상태 조회 실패: {e}")
            return {
                'error': f'상태 조회 실패: {str(e)}',
                'collected_count': self.collected_count,
                'error_count': self.error_count,
                'skipped_count': self.skipped_count
            }

    def cleanup_and_optimize(self) -> Dict[str, Any]:
        """데이터 정리 및 최적화"""
        try:
            print("🧹 데이터 정리 및 최적화 시작...")

            results = {
                'cleaned_duplicates': 0,
                'updated_metadata': 0,
                'optimized_tables': 0
            }

            # 1. 모든 활성 종목의 메타데이터 업데이트
            active_stocks = self.db_service.metadata_manager.get_all_active_stocks()

            for stock in active_stocks:
                stock_code = stock['code']

                # 메타데이터 업데이트
                if self.db_service.metadata_manager.update_stock_stats(stock_code):
                    results['updated_metadata'] += 1

                # 테이블 최적화 (SQLite VACUUM - 주의해서 사용)
                # results['optimized_tables'] += 1

            print(f"✅ 정리 완료: 메타데이터 {results['updated_metadata']}개 업데이트")

            return results

        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")
            return {'error': f'데이터 정리 실패: {str(e)}'}


# 편의 함수들 (향상된 버전)
def collect_daily_price_single(stock_code: str, config: Optional[Config] = None) -> bool:
    """단일 종목 일봉 데이터 수집 (편의 함수)"""
    collector = EnhancedDailyPriceCollector(config)

    if not collector.connect_kiwoom():
        return False

    return collector.collect_single_stock(stock_code)


def collect_daily_price_batch(stock_codes: List[str], config: Optional[Config] = None,
                             validate_data: bool = False) -> Dict[str, Any]:
    """배치 일봉 데이터 수집 (편의 함수)"""
    collector = EnhancedDailyPriceCollector(config)

    if not collector.connect_kiwoom():
        return {'error': '키움 API 연결 실패'}

    return collector.collect_multiple_stocks(stock_codes, validate_data=validate_data)


def collect_major_stocks_auto() -> Dict[str, Any]:
    """주요 종목 자동 설정 및 수집 (편의 함수)"""
    collector = EnhancedDailyPriceCollector()

    if not collector.connect_kiwoom():
        return {'error': '키움 API 연결 실패'}

    return collector.setup_and_collect_major_stocks()


def collect_all_active_stocks(validate_data: bool = True) -> Dict[str, Any]:
    """모든 활성 종목 수집 (편의 함수)"""
    collector = EnhancedDailyPriceCollector()

    if not collector.connect_kiwoom():
        return {'error': '키움 API 연결 실패'}

    return collector.collect_all_registered_stocks(validate_data=validate_data)


def setup_full_market_collection() -> Dict[str, Any]:
    """전체 시장 종목 등록 및 수집 준비"""
    try:
        print("🏢 전체 시장 종목 등록 시작...")

        # 1. 키움 API에서 전체 종목 등록
        from ..core.stock_manager import register_all_market_stocks
        registration_result = register_all_market_stocks()

        if 'error' in registration_result:
            return registration_result

        print(f"✅ 종목 등록 완료: {registration_result['success']}개")

        # 2. 수집기 준비
        collector = EnhancedDailyPriceCollector()

        if not collector.connect_kiwoom():
            return {'error': '키움 API 연결 실패'}

        print("✅ 전체 시장 수집 준비 완료")
        print("💡 이제 collect_all_active_stocks()로 전체 수집을 시작할 수 있습니다.")

        return {
            'registration_result': registration_result,
            'ready_for_collection': True,
            'message': '전체 시장 수집 준비 완료'
        }

    except Exception as e:
        logger.error(f"전체 시장 수집 준비 실패: {e}")
        return {'error': f'전체 시장 수집 준비 실패: {str(e)}'}


def run_daily_collection_with_validation() -> Dict[str, Any]:
    """일일 데이터 수집 + 품질 검증 (완전 자동화)"""
    try:
        print("🌅 일일 데이터 수집 및 검증 시작...")

        # 1. 데이터 수집
        collection_result = collect_all_active_stocks(validate_data=True)

        if 'error' in collection_result:
            return collection_result

        # 2. 품질 검증 리포트 생성
        from ..core.data_validator import run_full_data_validation
        validation_report = run_full_data_validation()

        # 3. 결과 요약
        result = {
            'collection_result': collection_result,
            'validation_report': validation_report,
            'completed_at': datetime.now().isoformat(),
            'summary': {
                'collected_stocks': len(collection_result.get('success', [])),
                'failed_stocks': len(collection_result.get('failed', [])),
                'total_records': collection_result.get('total_collected', 0),
                'elapsed_time': collection_result.get('elapsed_time', 0)
            }
        }

        print("🎉 일일 수집 및 검증 완료!")
        return result

    except Exception as e:
        logger.error(f"일일 수집 및 검증 실패: {e}")
        return {'error': f'일일 수집 및 검증 실패: {str(e)}'}


# 레거시 호환성을 위한 별칭
DailyPriceCollector = EnhancedDailyPriceCollector