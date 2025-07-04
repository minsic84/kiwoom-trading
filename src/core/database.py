"""
파일 경로: src/core/database.py

Enhanced Database System for Stock Trading
종목별 개별 테이블 구조 + 자동 메타데이터 관리 + 데이터 품질 검증
"""
import os
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import logging

from sqlalchemy import (
    create_engine, Column, Integer, String, BigInteger, Boolean,
    DateTime, VARCHAR, Index, UniqueConstraint, text, MetaData, Table
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from .config import Config

# 로거 설정
logger = logging.getLogger(__name__)

# SQLAlchemy Base
Base = declarative_base()


class Stock(Base):
    """확장된 주식 기본 정보 모델 (메타데이터 포함)"""
    __tablename__ = 'stocks'

    code = Column(VARCHAR(10), primary_key=True, comment='종목코드')
    name = Column(VARCHAR(100), nullable=False, comment='종목명')
    market = Column(VARCHAR(10), comment='시장구분(KOSPI/KOSDAQ)')

    # 메타데이터 필드들
    table_created = Column(Boolean, default=False, comment='일봉 테이블 생성 여부')
    last_updated = Column(DateTime, comment='마지막 데이터 업데이트')
    data_count = Column(Integer, default=0, comment='보유 일봉 데이터 개수')
    first_date = Column(VARCHAR(8), comment='첫 번째 데이터 날짜')
    latest_date = Column(VARCHAR(8), comment='최신 데이터 날짜')
    is_active = Column(Boolean, default=True, comment='활성 종목 여부')

    # 시스템 필드
    created_at = Column(DateTime, default=datetime.now, comment='종목 등록일')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, comment='정보 수정일')

    def __repr__(self):
        return f"<Stock(code='{self.code}', name='{self.name}', data_count={self.data_count})>"


class DynamicTableManager:
    """동적 테이블 생성 및 관리 클래스"""

    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

    def get_stock_table_name(self, stock_code: str) -> str:
        """종목 코드로 테이블명 생성"""
        return f"daily_prices_{stock_code}"

    def create_stock_daily_table(self, stock_code: str) -> bool:
        """종목별 일봉 데이터 테이블 생성"""
        try:
            table_name = self.get_stock_table_name(stock_code)

            # 이미 존재하는지 확인
            if self.check_stock_table_exists(stock_code):
                logger.info(f"테이블 {table_name} 이미 존재함")
                return True

            # 동적 테이블 정의
            daily_table = Table(
                table_name,
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('date', VARCHAR(8), nullable=False, comment='일자(YYYYMMDD)'),
                Column('start_price', Integer, comment='시가'),
                Column('high_price', Integer, comment='고가'),
                Column('low_price', Integer, comment='저가'),
                Column('current_price', Integer, comment='종가'),
                Column('volume', BigInteger, comment='거래량'),
                Column('trading_value', BigInteger, comment='거래대금'),
                Column('prev_day_diff', Integer, comment='전일대비', default=0),
                Column('change_rate', Integer, comment='등락율(소수점2자리*100)', default=0),
                Column('created_at', DateTime, default=datetime.now, comment='수집일시'),

                # 인덱스 설정
                Index(f'idx_{table_name}_date', 'date', unique=True),
                Index(f'idx_{table_name}_price', 'current_price'),
                Index(f'idx_{table_name}_volume', 'volume'),
            )

            # 테이블 생성
            daily_table.create(self.engine)
            logger.info(f"✅ 종목 {stock_code} 일봉 테이블 생성 완료: {table_name}")
            return True

        except Exception as e:
            logger.error(f"❌ 종목 {stock_code} 테이블 생성 실패: {e}")
            return False

    def check_stock_table_exists(self, stock_code: str) -> bool:
        """종목 테이블 존재 여부 확인"""
        try:
            table_name = self.get_stock_table_name(stock_code)

            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"
                ), {"table_name": table_name})
                return result.fetchone() is not None

        except Exception as e:
            logger.error(f"테이블 존재 여부 확인 실패: {e}")
            return False

    def drop_stock_table(self, stock_code: str) -> bool:
        """종목 테이블 삭제"""
        try:
            table_name = self.get_stock_table_name(stock_code)

            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()

            logger.info(f"종목 {stock_code} 테이블 삭제 완료")
            return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 테이블 삭제 실패: {e}")
            return False

    def get_all_stock_tables(self) -> List[str]:
        """모든 종목 테이블 목록 조회"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'daily_prices_%'"
                ))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"종목 테이블 목록 조회 실패: {e}")
            return []


class StockMetadataManager:
    """종목 메타데이터 자동 관리 클래스"""

    def __init__(self, session_factory):
        self.SessionLocal = session_factory

    def register_stock(self, stock_code: str, name: str = None, market: str = None) -> bool:
        """종목 정보 등록 또는 업데이트"""
        try:
            with self.SessionLocal() as session:
                # 기존 종목 확인
                existing = session.query(Stock).filter(Stock.code == stock_code).first()

                if existing:
                    # 기존 종목 정보 업데이트
                    if name:
                        existing.name = name
                    if market:
                        existing.market = market
                    existing.updated_at = datetime.now()
                    logger.info(f"종목 {stock_code} 정보 업데이트")
                else:
                    # 새 종목 등록
                    new_stock = Stock(
                        code=stock_code,
                        name=name or f"종목_{stock_code}",
                        market=market or "UNKNOWN",
                        table_created=False,
                        is_active=True
                    )
                    session.add(new_stock)
                    logger.info(f"새 종목 등록: {stock_code} - {name}")

                session.commit()
                return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 등록 실패: {e}")
            return False

    def mark_table_created(self, stock_code: str) -> bool:
        """테이블 생성 완료 표시"""
        try:
            with self.SessionLocal() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()
                if stock:
                    stock.table_created = True
                    stock.updated_at = datetime.now()
                    session.commit()
                    return True
                return False
        except Exception as e:
            logger.error(f"테이블 생성 표시 실패: {e}")
            return False

    def update_stock_stats(self, stock_code: str) -> bool:
        """종목 통계 정보 업데이트 (데이터 개수, 날짜 범위 등)"""
        try:
            with self.SessionLocal() as session:
                stock = session.query(Stock).filter(Stock.code == stock_code).first()
                if not stock:
                    return False

                # 동적 테이블에서 통계 조회
                table_name = f"daily_prices_{stock_code}"

                # 데이터 개수 조회
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                count_result = session.execute(count_query).fetchone()
                data_count = count_result[0] if count_result else 0

                # 날짜 범위 조회
                if data_count > 0:
                    date_query = text(f"""
                        SELECT MIN(date) as first_date, MAX(date) as latest_date 
                        FROM {table_name}
                    """)
                    date_result = session.execute(date_query).fetchone()
                    first_date = date_result[0] if date_result else None
                    latest_date = date_result[1] if date_result else None
                else:
                    first_date = None
                    latest_date = None

                # 메타데이터 업데이트
                stock.data_count = data_count
                stock.first_date = first_date
                stock.latest_date = latest_date
                stock.last_updated = datetime.now()
                stock.updated_at = datetime.now()

                session.commit()
                logger.info(f"종목 {stock_code} 통계 업데이트: {data_count}개 데이터")
                return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 통계 업데이트 실패: {e}")
            return False

    def get_all_active_stocks(self) -> List[Dict[str, Any]]:
        """모든 활성 종목 정보 조회"""
        try:
            with self.SessionLocal() as session:
                stocks = session.query(Stock).filter(Stock.is_active == True).all()

                return [{
                    'code': stock.code,
                    'name': stock.name,
                    'market': stock.market,
                    'table_created': stock.table_created,
                    'data_count': stock.data_count,
                    'latest_date': stock.latest_date,
                    'last_updated': stock.last_updated
                } for stock in stocks]

        except Exception as e:
            logger.error(f"활성 종목 조회 실패: {e}")
            return []

    def get_collection_status(self) -> Dict[str, Any]:
        """전체 수집 현황 조회"""
        try:
            with self.SessionLocal() as session:
                total_stocks = session.query(Stock).filter(Stock.is_active == True).count()
                created_tables = session.query(Stock).filter(
                    Stock.is_active == True,
                    Stock.table_created == True
                ).count()
                total_data = session.query(Stock).filter(Stock.is_active == True).with_entities(
                    Stock.data_count
                ).all()

                total_records = sum([count[0] or 0 for count in total_data])

                return {
                    'total_stocks': total_stocks,
                    'created_tables': created_tables,
                    'total_records': total_records,
                    'completion_rate': (created_tables / total_stocks * 100) if total_stocks > 0 else 0
                }

        except Exception as e:
            logger.error(f"수집 현황 조회 실패: {e}")
            return {}


class DatabaseManager:
    """향상된 데이터베이스 연결 및 관리 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.engine = None
        self.SessionLocal = None
        self.table_manager = None
        self.metadata_manager = None
        self._setup_database()

    def _setup_database(self):
        """데이터베이스 설정 및 연결"""
        try:
            database_url = self._get_database_url()
            logger.info(f"Database URL: {database_url}")

            # SQLAlchemy 엔진 생성
            self.engine = create_engine(
                database_url,
                echo=self.config.debug,
                pool_timeout=30,
                pool_recycle=3600
            )

            # 세션 팩토리 생성
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            # 관리자 클래스들 초기화
            self.table_manager = DynamicTableManager(self.engine)
            self.metadata_manager = StockMetadataManager(self.SessionLocal)

            # 데이터 디렉토리 생성 (SQLite용)
            if self.config.env == 'development':
                data_dir = Path('./data')
                data_dir.mkdir(exist_ok=True)

            logger.info("Enhanced database setup completed successfully")

        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise

    def _get_database_url(self) -> str:
        """환경에 따른 데이터베이스 URL 반환"""
        db_type = os.getenv('DB_TYPE', 'sqlite')

        if db_type == 'sqlite':
            db_path = os.getenv('SQLITE_DB_PATH', './data/stock_data.db')
            return f"sqlite:///{db_path}"
        elif db_type == 'postgresql':
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '5432')
            name = os.getenv('DB_NAME', 'stock_db')
            user = os.getenv('DB_USER', '')
            password = os.getenv('DB_PASSWORD', '')
            return f"postgresql://{user}:{password}@{host}:{port}/{name}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def create_tables(self):
        """기본 테이블 생성 (stocks 테이블만)"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("기본 테이블 (stocks) 생성 완료")
        except SQLAlchemyError as e:
            logger.error(f"테이블 생성 실패: {e}")
            raise

    def get_session(self) -> Session:
        """새 데이터베이스 세션 반환"""
        if not self.SessionLocal:
            raise RuntimeError("Database not initialized")
        return self.SessionLocal()

    def test_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False


class EnhancedDatabaseService:
    """향상된 데이터베이스 서비스 클래스"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.table_manager = db_manager.table_manager
        self.metadata_manager = db_manager.metadata_manager

    def prepare_stock_for_collection(self, stock_code: str, name: str = None, market: str = None) -> bool:
        """종목 수집 준비 (등록 + 테이블 생성)"""
        try:
            # 1. 종목 등록
            if not self.metadata_manager.register_stock(stock_code, name, market):
                return False

            # 2. 테이블 생성
            if not self.table_manager.create_stock_daily_table(stock_code):
                return False

            # 3. 메타데이터 업데이트
            self.metadata_manager.mark_table_created(stock_code)

            logger.info(f"종목 {stock_code} 수집 준비 완료")
            return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 수집 준비 실패: {e}")
            return False

    def add_daily_price_to_stock(self, stock_code: str, date: str,
                                current_price: int, volume: int, trading_value: int,
                                start_price: int, high_price: int, low_price: int,
                                prev_day_diff: int = 0, change_rate: float = 0.0) -> bool:
        """종목별 테이블에 일봉 데이터 추가"""
        try:
            # 테이블이 없으면 생성
            if not self.table_manager.check_stock_table_exists(stock_code):
                if not self.prepare_stock_for_collection(stock_code):
                    return False

            table_name = self.table_manager.get_stock_table_name(stock_code)
            change_rate_int = int(change_rate * 100) if change_rate is not None else 0

            with self.db_manager.get_session() as session:
                # 중복 데이터 확인
                check_query = text(f"SELECT COUNT(*) FROM {table_name} WHERE date = :date")
                existing = session.execute(check_query, {"date": date}).fetchone()

                if existing and existing[0] > 0:
                    # 기존 데이터 업데이트
                    update_query = text(f"""
                        UPDATE {table_name} SET
                            start_price = :start_price,
                            high_price = :high_price,
                            low_price = :low_price,
                            current_price = :current_price,
                            volume = :volume,
                            trading_value = :trading_value,
                            prev_day_diff = :prev_day_diff,
                            change_rate = :change_rate,
                            created_at = :created_at
                        WHERE date = :date
                    """)
                    session.execute(update_query, {
                        "date": date,
                        "start_price": start_price,
                        "high_price": high_price,
                        "low_price": low_price,
                        "current_price": current_price,
                        "volume": volume,
                        "trading_value": trading_value,
                        "prev_day_diff": prev_day_diff,
                        "change_rate": change_rate_int,
                        "created_at": datetime.now()
                    })
                else:
                    # 새 데이터 삽입
                    insert_query = text(f"""
                        INSERT INTO {table_name} 
                        (date, start_price, high_price, low_price, current_price, 
                         volume, trading_value, prev_day_diff, change_rate, created_at)
                        VALUES (:date, :start_price, :high_price, :low_price, :current_price,
                               :volume, :trading_value, :prev_day_diff, :change_rate, :created_at)
                    """)
                    session.execute(insert_query, {
                        "date": date,
                        "start_price": start_price,
                        "high_price": high_price,
                        "low_price": low_price,
                        "current_price": current_price,
                        "volume": volume,
                        "trading_value": trading_value,
                        "prev_day_diff": prev_day_diff,
                        "change_rate": change_rate_int,
                        "created_at": datetime.now()
                    })

                session.commit()

                # 메타데이터 업데이트
                self.metadata_manager.update_stock_stats(stock_code)
                return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 일봉 데이터 저장 실패: {e}")
            return False

    def get_stock_latest_date(self, stock_code: str) -> Optional[str]:
        """종목의 최신 데이터 날짜 조회"""
        try:
            if not self.table_manager.check_stock_table_exists(stock_code):
                return None

            table_name = self.table_manager.get_stock_table_name(stock_code)

            with self.db_manager.get_session() as session:
                query = text(f"SELECT MAX(date) FROM {table_name}")
                result = session.execute(query).fetchone()
                return result[0] if result and result[0] else None

        except Exception as e:
            logger.error(f"종목 {stock_code} 최신 날짜 조회 실패: {e}")
            return None


# 싱글톤 패턴으로 데이터베이스 매니저 인스턴스 생성
_db_manager: Optional[DatabaseManager] = None

def get_database_manager() -> DatabaseManager:
    """데이터베이스 매니저 싱글톤 인스턴스 반환"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager

def get_database_service() -> EnhancedDatabaseService:
    """향상된 데이터베이스 서비스 인스턴스 반환"""
    return EnhancedDatabaseService(get_database_manager())