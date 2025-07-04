"""
파일 경로: src/core/stock_manager.py

키움 API를 통한 전체 종목 정보 관리
전체 종목 리스트 조회 및 자동 등록 시스템
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import time

from .config import Config
from .database import get_database_service
from ..api.connector import KiwoomAPIConnector

# 로거 설정
logger = logging.getLogger(__name__)


class KiwoomStockManager:
    """키움 API를 통한 종목 정보 관리 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.kiwoom = None
        self.db_service = get_database_service()

        # 키움 API 시장 코드
        self.MARKET_CODES = {
            "0": "KOSPI",  # 코스피
            "10": "KOSDAQ",  # 코스닥
            "3": "ELW",  # ELW
            "8": "ETF",  # ETF
            "50": "KONEX"  # 코넥스
        }

        # 수집 대상 시장 (주식만)
        self.TARGET_MARKETS = ["0", "10"]  # KOSPI, KOSDAQ

    def connect_kiwoom(self) -> bool:
        """키움 API 연결"""
        try:
            from ..api.connector import get_kiwoom_connector

            self.kiwoom = get_kiwoom_connector(self.config)

            if not self.kiwoom.is_connected:
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

    def get_market_stock_list(self, market_code: str) -> List[Dict[str, str]]:
        """특정 시장의 종목 리스트 조회 (키움 API)"""
        try:
            if not self.kiwoom or not self.kiwoom.is_connected:
                logger.error("키움 API가 연결되지 않음")
                return []

            print(f"🔍 {self.MARKET_CODES.get(market_code, market_code)} 시장 종목 조회 중...")

            # 키움 API: GetCodeListByMarket 사용
            code_list = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", market_code)

            if not code_list:
                logger.warning(f"시장 {market_code} 종목 리스트가 비어있음")
                return []

            # 종목 코드 리스트 파싱 (세미콜론으로 구분)
            stock_codes = [code.strip() for code in code_list.split(';') if code.strip()]

            print(f"📊 {len(stock_codes)}개 종목 발견")

            # 각 종목의 상세 정보 조회
            stock_list = []
            for i, stock_code in enumerate(stock_codes[:100]):  # 테스트용으로 100개만
                try:
                    # 종목명 조회
                    stock_name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", stock_code)

                    if stock_name and stock_name.strip():
                        stock_info = {
                            "code": stock_code,
                            "name": stock_name.strip(),
                            "market": self.MARKET_CODES.get(market_code, "UNKNOWN")
                        }
                        stock_list.append(stock_info)

                        if (i + 1) % 50 == 0:
                            print(f"  진행률: {i + 1}/{len(stock_codes)}")

                    # API 요청 제한 대기
                    if i > 0 and i % 10 == 0:
                        time.sleep(0.1)  # 100ms 대기

                except Exception as e:
                    logger.warning(f"종목 {stock_code} 정보 조회 실패: {e}")
                    continue

            logger.info(f"시장 {market_code} 종목 조회 완료: {len(stock_list)}개")
            return stock_list

        except Exception as e:
            logger.error(f"시장 {market_code} 종목 리스트 조회 실패: {e}")
            return []

    def get_all_stocks(self) -> List[Dict[str, str]]:
        """전체 시장 종목 리스트 조회"""
        try:
            if not self.connect_kiwoom():
                return []

            all_stocks = []

            for market_code in self.TARGET_MARKETS:
                market_name = self.MARKET_CODES[market_code]
                print(f"\n📈 {market_name} 시장 종목 수집 중...")

                market_stocks = self.get_market_stock_list(market_code)
                all_stocks.extend(market_stocks)

                print(f"✅ {market_name}: {len(market_stocks)}개 종목 수집 완료")

                # 시장 간 대기
                time.sleep(1.0)

            logger.info(f"전체 종목 조회 완료: {len(all_stocks)}개")
            return all_stocks

        except Exception as e:
            logger.error(f"전체 종목 조회 실패: {e}")
            return []

    def register_all_stocks_to_db(self, stock_list: List[Dict[str, str]] = None) -> Dict[str, Any]:
        """전체 종목을 데이터베이스에 등록"""
        try:
            if stock_list is None:
                print("📋 키움 API에서 전체 종목 리스트 조회 중...")
                stock_list = self.get_all_stocks()

            if not stock_list:
                return {"success": 0, "failed": 0, "error": "종목 리스트가 비어있음"}

            print(f"💾 {len(stock_list)}개 종목을 데이터베이스에 등록 중...")

            success_count = 0
            failed_count = 0

            for i, stock_info in enumerate(stock_list):
                try:
                    stock_code = stock_info["code"]
                    stock_name = stock_info["name"]
                    market = stock_info["market"]

                    # 데이터베이스에 종목 등록
                    if self.db_service.metadata_manager.register_stock(stock_code, stock_name, market):
                        success_count += 1
                    else:
                        failed_count += 1
                        logger.warning(f"종목 등록 실패: {stock_code}")

                    # 진행률 표시
                    if (i + 1) % 100 == 0:
                        print(f"  📊 진행률: {i + 1}/{len(stock_list)} ({success_count}개 성공)")

                except Exception as e:
                    failed_count += 1
                    logger.error(f"종목 등록 중 오류: {e}")

            result = {
                "success": success_count,
                "failed": failed_count,
                "total": len(stock_list)
            }

            print(f"\n✅ 종목 등록 완료:")
            print(f"   📈 성공: {success_count}개")
            print(f"   ❌ 실패: {failed_count}개")
            print(f"   📊 전체: {len(stock_list)}개")

            return result

        except Exception as e:
            logger.error(f"종목 등록 실패: {e}")
            return {"success": 0, "failed": 0, "error": str(e)}

    def update_stock_info(self, stock_code: str) -> bool:
        """특정 종목 정보 업데이트"""
        try:
            if not self.kiwoom or not self.kiwoom.is_connected:
                return False

            # 종목명 조회
            stock_name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", stock_code)
            if not stock_name:
                return False

            # 시장 구분 조회 (추가 구현 필요)
            market = "UNKNOWN"  # 기본값

            # 데이터베이스 업데이트
            return self.db_service.metadata_manager.register_stock(stock_code, stock_name.strip(), market)

        except Exception as e:
            logger.error(f"종목 {stock_code} 정보 업데이트 실패: {e}")
            return False

    def get_kospi_top_stocks(self, count: int = 10) -> List[str]:
        """코스피 대형주 상위 N개 종목 코드 반환"""
        # 주요 대형주 종목들 (시가총액 기준)
        kospi_major_stocks = [
            "005930",  # 삼성전자
            "000660",  # SK하이닉스
            "035420",  # NAVER
            "005380",  # 현대차
            "006400",  # 삼성SDI
            "051910",  # LG화학
            "068270",  # 셀트리온
            "035720",  # 카카오
            "005490",  # POSCO홀딩스
            "012330",  # 현대모비스
            "028260",  # 삼성물산
            "066570",  # LG전자
            "015760",  # 한국전력
            "033780",  # KT&G
            "003550",  # LG
            "096770",  # SK이노베이션
            "017670",  # SK텔레콤
            "034020",  # 두산에너빌리티
            "003490",  # 대한항공
            "009150"  # 삼성전기
        ]

        return kospi_major_stocks[:count]

    def get_kosdaq_top_stocks(self, count: int = 5) -> List[str]:
        """코스닥 대형주 상위 N개 종목 코드 반환"""
        kosdaq_major_stocks = [
            "091990",  # 셀트리온헬스케어
            "086900",  # 메디톡스
            "196170",  # 알테오젠
            "065350",  # 신성델타테크
            "263750"  # 펄어비스
        ]

        return kosdaq_major_stocks[:count]

    def setup_major_stocks_for_testing(self) -> List[str]:
        """테스트용 주요 종목 설정"""
        major_stocks = [
            ("005930", "삼성전자", "KOSPI"),
            ("000660", "SK하이닉스", "KOSPI"),
            ("035420", "NAVER", "KOSPI"),
            ("005380", "현대차", "KOSPI"),
            ("068270", "셀트리온", "KOSPI")
        ]

        stock_codes = []

        for stock_code, stock_name, market in major_stocks:
            # 데이터베이스에 등록
            if self.db_service.metadata_manager.register_stock(stock_code, stock_name, market):
                stock_codes.append(stock_code)
                logger.info(f"테스트용 종목 등록: {stock_code} - {stock_name}")

        return stock_codes


def create_stock_manager(config: Optional[Config] = None) -> KiwoomStockManager:
    """종목 관리자 인스턴스 생성"""
    return KiwoomStockManager(config)


def setup_test_stocks() -> List[str]:
    """테스트용 주요 종목 자동 설정"""
    manager = create_stock_manager()
    return manager.setup_major_stocks_for_testing()


def register_all_market_stocks() -> Dict[str, Any]:
    """전체 시장 종목 자동 등록 (실제 키움 API 사용)"""
    manager = create_stock_manager()
    return manager.register_all_stocks_to_db()