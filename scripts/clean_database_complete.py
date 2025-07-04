#!/usr/bin/env python3
"""
파일 경로: scripts/clean_database_complete.py

데이터베이스 완전 초기화 및 새 구조 생성 스크립트
기존 모든 테이블 삭제 후 새로운 종목별 테이블 구조로 재구성
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def complete_database_reset():
    """데이터베이스 완전 초기화"""
    print("🗑️ 데이터베이스 완전 초기화 시작...")
    print("=" * 50)

    # 1. SQLite DB 파일 완전 삭제
    db_files = [
        project_root / "data" / "stock_data.db",
        project_root / "data" / "stock_data_dev.db",
        project_root / "scripts" / "data" / "stock_data_dev.db"
    ]

    for db_path in db_files:
        if db_path.exists():
            db_path.unlink()
            print(f"✅ 삭제 완료: {db_path}")

    # 2. data 폴더 재생성
    data_dir = project_root / "data"
    data_dir.mkdir(exist_ok=True)
    print("✅ data 폴더 준비 완료")

    # 3. 로그 파일 정리 (선택적)
    logs_dir = project_root / "logs"
    if logs_dir.exists():
        for log_file in logs_dir.glob("*.log"):
            try:
                log_file.unlink()
                print(f"✅ 로그 파일 삭제: {log_file}")
            except:
                pass

    print("\n🎉 데이터베이스 완전 초기화 완료!")
    print("💡 이제 새로운 구조로 테이블을 생성합니다...")


def create_new_structure():
    """새로운 데이터베이스 구조 생성"""
    try:
        from src.core.database import get_database_manager

        print("\n🏗️ 새로운 데이터베이스 구조 생성 중...")

        db_manager = get_database_manager()
        db_manager.create_tables()

        print("✅ 새로운 테이블 구조 생성 완료!")

        # 연결 테스트
        if db_manager.test_connection():
            print("✅ 데이터베이스 연결 테스트 성공!")
        else:
            print("❌ 데이터베이스 연결 테스트 실패!")

    except Exception as e:
        print(f"❌ 새 구조 생성 실패: {e}")


def main():
    """메인 함수"""
    print("🚀 주식 트레이딩 시스템 - 데이터베이스 완전 재구성")
    print("=" * 60)
    print("⚠️  주의: 모든 기존 데이터가 완전히 삭제됩니다!")

    response = input("\n계속 진행하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ 작업이 취소되었습니다.")
        return

    # 1단계: 완전 초기화
    complete_database_reset()

    # 2단계: 새 구조 생성
    create_new_structure()

    print("\n" + "=" * 60)
    print("🎉 데이터베이스 재구성 완료!")
    print("💡 이제 새로운 수집기로 데이터를 수집할 수 있습니다.")
    print("📋 다음 단계: python scripts/test_enhanced_collector.py")


if __name__ == "__main__":
    main()