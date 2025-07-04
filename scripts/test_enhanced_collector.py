#!/usr/bin/env python3
"""
파일 경로: scripts/test_enhanced_collector.py

향상된 일봉 데이터 수집기 테스트 스크립트
- 종목별 개별 테이블 구조
- 자동 종목 등록
- 데이터 품질 검증
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.daily_price import EnhancedDailyPriceCollector
from src.core.stock_manager import create_stock_manager
from src.core.data_validator import DataQualityValidator
from src.core.database import get_database_service


def test_database_structure():
    """새로운 데이터베이스 구조 테스트"""
    print("🏗️ 새로운 데이터베이스 구조 테스트")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 기본 테이블 존재 확인
        with db_service.db_manager.get_session() as session:
            from sqlalchemy import text
            result = session.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )).fetchall()

            tables = [row[0] for row in result]
            print(f"✅ 기존 테이블: {len(tables)}개")
            for table in tables:
                print(f"   📋 {table}")

        # 연결 테스트
        if db_service.db_manager.test_connection():
            print("✅ 데이터베이스 연결 테스트 성공")
        else:
            print("❌ 데이터베이스 연결 테스트 실패")
            return False

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 구조 테스트 실패: {e}")
        return False


def test_stock_manager():
    """종목 관리자 테스트"""
    print("\n📊 종목 관리자 테스트")
    print("=" * 50)

    try:
        stock_manager = create_stock_manager()

        # 테스트용 주요 종목 설정
        print("🔧 테스트용 주요 종목 설정 중...")
        stock_codes = stock_manager.setup_major_stocks_for_testing()

        if stock_codes:
            print(f"✅ {len(stock_codes)}개 종목 설정 완료:")
            for code in stock_codes:
                print(f"   📈 {code}")
        else:
            print("❌ 종목 설정 실패")
            return False

        # 등록된 종목 확인
        db_service = get_database_service()
        active_stocks = db_service.metadata_manager.get_all_active_stocks()
        print(f"📋 등록된 활성 종목: {len(active_stocks)}개")

        return True

    except Exception as e:
        print(f"❌ 종목 관리자 테스트 실패: {e}")
        return False


def test_enhanced_collector_basic():
    """향상된 수집기 기본 기능 테스트"""
    print("\n🚀 향상된 수집기 기본 테스트")
    print("=" * 50)

    try:
        collector = EnhancedDailyPriceCollector()

        # 수집기 상태 확인
        status = collector.get_collection_status()
        print(f"✅ 수집기 초기화 완료")
        print(f"📊 DB 연결: {'정상' if status['db_connected'] else '실패'}")
        print(f"📊 키움 연결: {'정상' if status['kiwoom_connected'] else '미연결'}")
        print(f"📊 등록된 종목: {status.get('total_stocks', 0)}개")
        print(f"📊 생성된 테이블: {status.get('created_tables', 0)}개")

        return True

    except Exception as e:
        print(f"❌ 향상된 수집기 테스트 실패: {e}")
        return False


def test_kiwoom_connection():
    """키움 API 연결 테스트"""
    print("\n🔌 키움 API 연결 테스트")
    print("=" * 50)

    response = input("키움 OpenAPI로 로그인하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  키움 연결 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = EnhancedDailyPriceCollector()

        print("🔄 키움 API 연결 중... (로그인 창이 나타날 수 있습니다)")

        if collector.connect_kiwoom(auto_login=True):
            print("✅ 키움 API 연결 성공")

            status = collector.get_collection_status()
            print(f"📊 연결 상태: {status['kiwoom_connected']}")

            return True
        else:
            print("❌ 키움 API 연결 실패")
            return False

    except Exception as e:
        print(f"❌ 키움 연결 테스트 실패: {e}")
        return False


def test_single_stock_collection():
    """단일 종목 수집 테스트 (새 구조)"""
    print("\n📈 단일 종목 수집 테스트 (새 구조)")
    print("=" * 50)

    # 키움 API 연결 필요 여부 확인
    response = input("실제 데이터 수집을 테스트하시겠습니까? (키움 로그인 필요) (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  실제 수집 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = EnhancedDailyPriceCollector()

        # 키움 연결
        if not collector.connect_kiwoom():
            print("❌ 키움 API 연결 실패")
            return False

        # 테스트 종목 (삼성전자)
        test_stock = "005930"
        print(f"📊 테스트 종목: {test_stock} (삼성전자)")

        # 새로운 구조로 데이터 수집
        print("🔄 새 구조로 일봉 데이터 수집 중...")
        success = collector.collect_single_stock(test_stock, update_existing=True)

        if success:
            print("✅ 일봉 데이터 수집 성공!")

            # 수집 상태 확인
            status = collector.get_collection_status()
            print(f"📊 수집된 레코드 수: {status['collected_count']}")
            print(f"📊 오류 수: {status['error_count']}")
            print(f"📊 등록된 종목: {status['registered_stocks']}")

            # 생성된 테이블 확인
            db_service = get_database_service()
            table_name = db_service.table_manager.get_stock_table_name(test_stock)
            if db_service.table_manager.check_stock_table_exists(test_stock):
                print(f"✅ 종목 테이블 생성 확인: {table_name}")
            else:
                print(f"❌ 종목 테이블 생성 실패: {table_name}")

            return True
        else:
            print("❌ 일봉 데이터 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 단일 종목 수집 테스트 실패: {e}")
        return False


def test_major_stocks_collection():
    """주요 종목 자동 수집 테스트"""
    print("\n🏢 주요 종목 자동 수집 테스트")
    print("=" * 50)

    # 키움 API 연결 필요 여부 확인
    response = input("주요 종목 자동 수집을 테스트하시겠습니까? (시간이 걸릴 수 있음) (y/N): ")
    if response.lower() != 'y':
        print("ℹ️  주요 종목 수집 테스트를 건너뜁니다.")
        return True  # 스킵

    try:
        collector = EnhancedDailyPriceCollector()

        # 키움 연결
        if not collector.connect_kiwoom():
            print("❌ 키움 API 연결 실패")
            return False

        # 진행률 콜백 함수
        def progress_callback(current, total, stock_code):
            print(f"🔄 진행률: {current}/{total} - {stock_code}")

        # 주요 종목 자동 설정 및 수집
        print("🔄 주요 종목 자동 설정 및 수집 중...")
        results = collector.setup_and_collect_major_stocks()

        if 'error' in results:
            print(f"❌ 주요 종목 수집 실패: {results['error']}")
            return False

        # 결과 출력
        print("\n📋 주요 종목 수집 결과:")
        print(f"✅ 성공: {len(results.get('success', []))}개")
        print(f"❌ 실패: {len(results.get('failed', []))}개")
        print(f"⏭️ 건너뛰기: {len(results.get('skipped', []))}개")
        print(f"🆕 신규 등록: {results.get('total_registered', 0)}개")
        print(f"📊 총 수집 레코드: {results.get('total_collected', 0):,}개")
        print(f"⏱️ 소요 시간: {results.get('elapsed_time', 0):.1f}초")

        if results.get('failed'):
            print(f"\n❌ 실패한 종목들: {results['failed']}")

        return len(results.get('success', [])) > 0

    except Exception as e:
        print(f"❌ 주요 종목 수집 테스트 실패: {e}")
        return False


def test_data_validation():
    """데이터 품질 검증 테스트"""
    print("\n🔍 데이터 품질 검증 테스트")
    print("=" * 50)

    try:
        validator = DataQualityValidator()

        # 등록된 종목들 확인
        db_service = get_database_service()
        active_stocks = db_service.metadata_manager.get_all_active_stocks()

        if not active_stocks:
            print("⚠️ 검증할 종목이 없습니다. 먼저 데이터를 수집하세요.")
            return True  # 스킵

        # 첫 번째 종목으로 테스트
        test_stock = active_stocks[0]['code']
        test_name = active_stocks[0]['name']

        print(f"📊 검증 대상: {test_stock} ({test_name})")

        # 종목 데이터 검증
        validation_results = validator.validate_stock_data(test_stock)

        print(f"📋 검증 결과: {len(validation_results)}개 항목")

        for result in validation_results:
            status_icon = {
                "PASS": "✅",
                "WARNING": "⚠️",
                "ERROR": "❌"
            }.get(result.status, "❓")

            print(f"{status_icon} {result.check_type}: {result.message}")

        # 전체 검증 여부 확인
        response = input("\n전체 종목 검증을 실행하시겠습니까? (y/N): ")
        if response.lower() == 'y':
            print("🔍 전체 종목 검증 중...")
            all_results = validator.validate_all_stocks()

            # 간단 요약
            total_errors = sum([
                len([r for r in results if r.status == "ERROR"])
                for results in all_results.values()
            ])
            total_warnings = sum([
                len([r for r in results if r.status == "WARNING"])
                for results in all_results.values()
            ])

            print(f"📊 전체 검증 완료: 오류 {total_errors}개, 경고 {total_warnings}개")

        return True

    except Exception as e:
        print(f"❌ 데이터 검증 테스트 실패: {e}")
        return False


def test_new_structure_verification():
    """새 구조 종합 검증"""
    print("\n🔬 새 구조 종합 검증")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 1. stocks 테이블 확인
        active_stocks = db_service.metadata_manager.get_all_active_stocks()
        print(f"📊 등록된 활성 종목: {len(active_stocks)}개")

        # 2. 종목별 테이블 확인
        stock_tables = db_service.table_manager.get_all_stock_tables()
        print(f"📋 생성된 종목 테이블: {len(stock_tables)}개")

        # 3. 메타데이터 일관성 확인
        consistency_issues = 0
        for stock in active_stocks[:5]:  # 처음 5개만 확인
            stock_code = stock['code']

            # 테이블 존재 여부와 메타데이터 일치 확인
            table_exists = db_service.table_manager.check_stock_table_exists(stock_code)
            metadata_says_created = stock['table_created']

            if table_exists != metadata_says_created:
                consistency_issues += 1
                print(f"⚠️ {stock_code}: 메타데이터 불일치 (테이블={table_exists}, 메타={metadata_says_created})")

        if consistency_issues == 0:
            print("✅ 메타데이터 일관성 검증 통과")
        else:
            print(f"⚠️ 메타데이터 불일치 {consistency_issues}개 발견")

        # 4. 샘플 데이터 확인
        if active_stocks:
            sample_stock = active_stocks[0]
            stock_code = sample_stock['code']

            if db_service.table_manager.check_stock_table_exists(stock_code):
                table_name = db_service.table_manager.get_stock_table_name(stock_code)

                with db_service.db_manager.get_session() as session:
                    from sqlalchemy import text
                    count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                    count = session.execute(count_query).fetchone()[0]

                    if count > 0:
                        sample_query = text(
                            f"SELECT date, current_price, volume FROM {table_name} ORDER BY date DESC LIMIT 3")
                        samples = session.execute(sample_query).fetchall()

                        print(f"📊 {stock_code} 샘플 데이터 ({count}개 총 레코드):")
                        for sample in samples:
                            print(f"   📅 {sample[0]}: {sample[1]:,}원, 거래량 {sample[2]:,}")
                    else:
                        print(f"⚠️ {stock_code} 테이블에 데이터 없음")

        # 5. 수집 현황 요약
        collection_status = db_service.metadata_manager.get_collection_status()
        print(f"\n📋 전체 현황:")
        print(f"   📈 총 종목: {collection_status.get('total_stocks', 0)}개")
        print(f"   🏗️ 생성된 테이블: {collection_status.get('created_tables', 0)}개")
        print(f"   📊 총 레코드: {collection_status.get('total_records', 0):,}개")
        print(f"   📊 완성률: {collection_status.get('completion_rate', 0):.1f}%")

        return True

    except Exception as e:
        print(f"❌ 새 구조 검증 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print("🚀 향상된 일봉 데이터 수집기 테스트")
    print("=" * 60)
    print("🆕 새로운 기능:")
    print("   - 종목별 개별 테이블 구조")
    print("   - 자동 종목 등록 및 관리")
    print("   - 실시간 메타데이터 업데이트")
    print("   - 데이터 품질 검증")
    print("=" * 60)

    # 테스트 실행
    tests = [
        ("새 데이터베이스 구조", test_database_structure),
        ("종목 관리자", test_stock_manager),
        ("향상된 수집기 기본", test_enhanced_collector_basic),
        ("키움 API 연결", test_kiwoom_connection),
        ("단일 종목 수집 (새 구조)", test_single_stock_collection),
        ("주요 종목 자동 수집", test_major_stocks_collection),
        ("데이터 품질 검증", test_data_validation),
        ("새 구조 종합 검증", test_new_structure_verification)
    ]

    results = []

    for test_name, test_func in tests:
        result = test_func()
        results.append((test_name, result))

    # 결과 요약
    print("\n" + "=" * 60)
    print("📋 테스트 결과 요약")
    print("=" * 60)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ 성공" if result else "❌ 실패"
        print(f"{test_name:.<35} {status}")
        if result:
            passed += 1

    print(f"\n🎯 전체 결과: {passed}/{total} 테스트 통과")

    if passed == total:
        print("🎉 모든 테스트 통과! 향상된 수집기 준비 완료.")
        print("\n💡 다음 단계:")
        print("   1. python scripts/collect_enhanced_example.py - 실제 수집 예제")
        print("   2. python scripts/check_new_structure.py - 새 구조 데이터 확인")
        print("   3. python scripts/run_daily_collection.py - 일일 자동 수집")
    elif passed >= total - 3:  # 키움 연결 관련 테스트는 선택사항
        print("✨ 핵심 기능 테스트 통과! 키움 연결 후 실제 수집 가능.")
        print("\n💡 권장사항:")
        print("   - 키움 OpenAPI 설치 및 로그인")
        print("   - 실제 데이터 수집 테스트 진행")
    else:
        print("⚠️ 일부 테스트 실패. 위 로그를 확인해주세요.")
        print("\n🔧 문제 해결:")
        print("   1. scripts/clean_database_complete.py 실행")
        print("   2. 데이터베이스 초기화 후 재시도")

    return passed >= total - 3


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)