#!/usr/bin/env python3
"""
파일 경로: scripts/check_new_structure.py

새로운 종목별 테이블 구조 데이터 확인 스크립트
HeidiSQL로 확인할 수 있는 쿼리들도 제공
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_database_service
from sqlalchemy import text
from datetime import datetime


def show_database_overview():
    """데이터베이스 전체 현황"""
    print("📊 데이터베이스 전체 현황")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 전체 테이블 조회
        with db_service.db_manager.get_session() as session:
            tables_query = text("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
            tables = session.execute(tables_query).fetchall()

            print(f"📋 전체 테이블: {len(tables)}개")

            # 기본 테이블과 종목 테이블 분류
            basic_tables = []
            stock_tables = []

            for table in tables:
                table_name = table[0]
                if table_name.startswith('daily_prices_'):
                    stock_tables.append(table_name)
                else:
                    basic_tables.append(table_name)

            print(f"   🏗️ 기본 테이블: {len(basic_tables)}개")
            for table in basic_tables:
                print(f"      📋 {table}")

            print(f"   📈 종목 테이블: {len(stock_tables)}개")
            if len(stock_tables) <= 10:
                for table in stock_tables:
                    stock_code = table.replace('daily_prices_', '')
                    print(f"      📊 {table} ({stock_code})")
            else:
                for table in stock_tables[:5]:
                    stock_code = table.replace('daily_prices_', '')
                    print(f"      📊 {table} ({stock_code})")
                print(f"      ... 및 {len(stock_tables) - 5}개 더")

        return True

    except Exception as e:
        print(f"❌ 데이터베이스 현황 조회 실패: {e}")
        return False


def show_stocks_metadata():
    """stocks 테이블 메타데이터 현황"""
    print("\n📈 종목 메타데이터 현황 (stocks 테이블)")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 전체 종목 현황
        active_stocks = db_service.metadata_manager.get_all_active_stocks()
        collection_status = db_service.metadata_manager.get_collection_status()

        print(f"📊 등록된 활성 종목: {len(active_stocks)}개")
        print(f"🏗️ 테이블 생성 완료: {collection_status.get('created_tables', 0)}개")
        print(f"📊 총 데이터 레코드: {collection_status.get('total_records', 0):,}개")
        print(f"📊 완성률: {collection_status.get('completion_rate', 0):.1f}%")

        if active_stocks:
            print(f"\n📋 종목별 상세 현황:")
            print(f"{'종목코드':<8} {'종목명':<15} {'테이블':<6} {'데이터수':<8} {'최신날짜':<10}")
            print("-" * 60)

            for stock in active_stocks[:20]:  # 최대 20개만 표시
                table_status = "✅" if stock['table_created'] else "❌"
                data_count = stock['data_count'] or 0
                latest_date = stock['latest_date'] or "없음"

                print(
                    f"{stock['code']:<8} {stock['name'][:14]:<15} {table_status:<6} {data_count:<8,} {latest_date:<10}")

            if len(active_stocks) > 20:
                print(f"... 및 {len(active_stocks) - 20}개 종목 더")

        return True

    except Exception as e:
        print(f"❌ 종목 메타데이터 조회 실패: {e}")
        return False


def show_sample_stock_data():
    """샘플 종목 데이터 상세 조회"""
    print("\n📊 샘플 종목 데이터 상세 조회")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 활성 종목 중 데이터가 있는 종목 찾기
        active_stocks = db_service.metadata_manager.get_all_active_stocks()

        sample_stock = None
        for stock in active_stocks:
            if stock['data_count'] and stock['data_count'] > 0:
                sample_stock = stock
                break

        if not sample_stock:
            print("⚠️ 데이터가 있는 종목을 찾을 수 없습니다.")
            return True

        stock_code = sample_stock['code']
        stock_name = sample_stock['name']
        table_name = db_service.table_manager.get_stock_table_name(stock_code)

        print(f"📈 샘플 종목: {stock_code} ({stock_name})")
        print(f"📋 테이블명: {table_name}")
        print(f"📊 총 데이터: {sample_stock['data_count']:,}개")

        # 최신 10개 데이터 조회
        with db_service.db_manager.get_session() as session:
            sample_query = text(f"""
                SELECT date, start_price, high_price, low_price, current_price, volume, trading_value
                FROM {table_name} 
                ORDER BY date DESC 
                LIMIT 10
            """)
            samples = session.execute(sample_query).fetchall()

            if samples:
                print(f"\n📅 최신 10개 데이터:")
                print(f"{'날짜':<10} {'시가':<8} {'고가':<8} {'저가':<8} {'종가':<8} {'거래량':<12} {'거래대금':<15}")
                print("-" * 80)

                for sample in samples:
                    print(
                        f"{sample[0]:<10} {sample[1]:<8,} {sample[2]:<8,} {sample[3]:<8,} {sample[4]:<8,} {sample[5]:<12,} {sample[6]:<15,}")

        # 통계 정보
        with db_service.db_manager.get_session() as session:
            stats_query = text(f"""
                SELECT 
                    MIN(date) as first_date,
                    MAX(date) as last_date,
                    AVG(current_price) as avg_price,
                    MIN(current_price) as min_price,
                    MAX(current_price) as max_price,
                    AVG(volume) as avg_volume
                FROM {table_name}
                WHERE current_price > 0
            """)
            stats = session.execute(stats_query).fetchone()

            if stats:
                print(f"\n📊 통계 정보:")
                print(f"   📅 데이터 기간: {stats[0]} ~ {stats[1]}")
                print(f"   💰 평균 주가: {int(stats[2]):,}원")
                print(f"   💰 최저 주가: {stats[3]:,}원")
                print(f"   💰 최고 주가: {stats[4]:,}원")
                print(f"   📊 평균 거래량: {int(stats[5]):,}주")

        return True

    except Exception as e:
        print(f"❌ 샘플 데이터 조회 실패: {e}")
        return False


def generate_heidisql_queries():
    """HeidiSQL에서 사용할 수 있는 쿼리 생성"""
    print("\n💻 HeidiSQL 사용 쿼리")
    print("=" * 50)

    try:
        db_service = get_database_service()

        print("📋 다음 쿼리들을 HeidiSQL에서 복사해서 실행하세요:")
        print()

        # 1. 전체 테이블 목록
        print("-- 1. 전체 테이블 목록 조회")
        print("SELECT name, type FROM sqlite_master WHERE type='table' ORDER BY name;")
        print()

        # 2. 종목 메타데이터 조회
        print("-- 2. 종목 메타데이터 현황")
        print("""
SELECT 
    code,
    name,
    market,
    table_created,
    data_count,
    latest_date,
    last_updated
FROM stocks 
WHERE is_active = 1
ORDER BY data_count DESC;
""")

        # 3. 샘플 종목 데이터
        active_stocks = db_service.metadata_manager.get_all_active_stocks()
        if active_stocks:
            sample_stock = None
            for stock in active_stocks:
                if stock['data_count'] and stock['data_count'] > 0:
                    sample_stock = stock
                    break

            if sample_stock:
                stock_code = sample_stock['code']
                table_name = f"daily_prices_{stock_code}"

                print(f"-- 3. {stock_code} ({sample_stock['name']}) 최신 데이터")
                print(f"""
SELECT 
    date,
    start_price,
    high_price,
    low_price,
    current_price,
    volume,
    trading_value
FROM {table_name}
ORDER BY date DESC
LIMIT 20;
""")

                print(f"-- 4. {stock_code} 통계 정보")
                print(f"""
SELECT 
    COUNT(*) as total_records,
    MIN(date) as first_date,
    MAX(date) as last_date,
    AVG(current_price) as avg_price,
    MIN(current_price) as min_price,
    MAX(current_price) as max_price,
    AVG(volume) as avg_volume
FROM {table_name}
WHERE current_price > 0;
""")

        # 5. 전체 데이터 현황
        print("-- 5. 전체 데이터 현황 요약")
        print("""
SELECT 
    COUNT(*) as total_stocks,
    SUM(CASE WHEN table_created = 1 THEN 1 ELSE 0 END) as created_tables,
    SUM(data_count) as total_records,
    AVG(data_count) as avg_records_per_stock
FROM stocks 
WHERE is_active = 1;
""")

        # 6. 종목별 테이블 크기
        print("-- 6. 종목별 데이터 개수 TOP 10")
        print("""
SELECT 
    code,
    name,
    data_count,
    latest_date
FROM stocks 
WHERE is_active = 1 AND data_count > 0
ORDER BY data_count DESC
LIMIT 10;
""")

        print("\n💡 사용법:")
        print("1. HeidiSQL에서 SQLite 파일 열기: data/stock_data.db")
        print("2. 위 쿼리들을 복사해서 실행")
        print("3. F9 키로 쿼리 실행")

        return True

    except Exception as e:
        print(f"❌ 쿼리 생성 실패: {e}")
        return False


def check_data_integrity():
    """데이터 무결성 검사"""
    print("\n🔍 데이터 무결성 검사")
    print("=" * 50)

    try:
        db_service = get_database_service()

        issues = []

        # 1. 메타데이터 일관성 검사
        active_stocks = db_service.metadata_manager.get_all_active_stocks()

        for stock in active_stocks[:10]:  # 처음 10개만 검사
            stock_code = stock['code']

            # 테이블 존재 여부와 메타데이터 비교
            actual_table_exists = db_service.table_manager.check_stock_table_exists(stock_code)
            metadata_says_created = stock['table_created']

            if actual_table_exists != metadata_says_created:
                issues.append(f"{stock_code}: 테이블 존재({actual_table_exists}) ≠ 메타데이터({metadata_says_created})")

            # 실제 데이터 개수와 메타데이터 비교
            if actual_table_exists:
                table_name = db_service.table_manager.get_stock_table_name(stock_code)
                with db_service.db_manager.get_session() as session:
                    count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                    actual_count = session.execute(count_query).fetchone()[0]
                    metadata_count = stock['data_count'] or 0

                    if abs(actual_count - metadata_count) > 0:
                        issues.append(f"{stock_code}: 실제 데이터({actual_count}) ≠ 메타데이터({metadata_count})")

        if issues:
            print(f"⚠️ 발견된 문제점 {len(issues)}개:")
            for issue in issues:
                print(f"   ❌ {issue}")
        else:
            print("✅ 데이터 무결성 검사 통과")

        # 2. 테이블 구조 검사 (샘플)
        if active_stocks:
            sample_stock = active_stocks[0]
            stock_code = sample_stock['code']

            if db_service.table_manager.check_stock_table_exists(stock_code):
                table_name = db_service.table_manager.get_stock_table_name(stock_code)

                with db_service.db_manager.get_session() as session:
                    # 테이블 스키마 확인
                    schema_query = text(f"PRAGMA table_info({table_name})")
                    columns = session.execute(schema_query).fetchall()

                    expected_columns = ['id', 'date', 'start_price', 'high_price', 'low_price',
                                        'current_price', 'volume', 'trading_value', 'prev_day_diff',
                                        'change_rate', 'created_at']

                    actual_columns = [col[1] for col in columns]

                    missing_columns = set(expected_columns) - set(actual_columns)
                    extra_columns = set(actual_columns) - set(expected_columns)

                    if missing_columns or extra_columns:
                        print(f"⚠️ {stock_code} 테이블 스키마 문제:")
                        if missing_columns:
                            print(f"   ❌ 누락된 컬럼: {missing_columns}")
                        if extra_columns:
                            print(f"   ⚠️ 추가 컬럼: {extra_columns}")
                    else:
                        print(f"✅ {stock_code} 테이블 스키마 정상")

        return True

    except Exception as e:
        print(f"❌ 무결성 검사 실패: {e}")
        return False


def main():
    """메인 함수"""
    print("🔍 새로운 종목별 테이블 구조 데이터 확인")
    print("=" * 60)

    # 검사 항목들
    checks = [
        ("데이터베이스 전체 현황", show_database_overview),
        ("종목 메타데이터 현황", show_stocks_metadata),
        ("샘플 종목 데이터", show_sample_stock_data),
        ("데이터 무결성 검사", check_data_integrity),
        ("HeidiSQL 쿼리 생성", generate_heidisql_queries)
    ]

    for check_name, check_func in checks:
        print(f"\n{'-' * 20} {check_name} {'-' * 20}")
        try:
            check_func()
        except Exception as e:
            print(f"❌ {check_name} 실행 중 오류: {e}")

    print(f"\n{'=' * 60}")
    print("🎉 새 구조 데이터 확인 완료!")
    print("\n💡 다음 단계:")
    print("   1. HeidiSQL로 위의 쿼리들 실행해보기")
    print("   2. 데이터 품질 검증: python scripts/validate_data_quality.py")
    print("   3. 추가 데이터 수집: python scripts/collect_enhanced_example.py")


if __name__ == "__main__":
    main()