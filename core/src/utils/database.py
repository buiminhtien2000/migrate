import os 
import json
from typing import Optional, Any, Dict, List, Union
import asyncpg # type: ignore
import asyncio
from contextlib import asynccontextmanager

class Database:
    def __init__(self, config: 'Config', logger: 'CustomLogger'): # type: ignore
        self.config = config
        self.logger = logger
        self.pool: Optional[asyncpg.Pool] = None

    async def create_database(self):
        """Create database if not exists"""
        try:
            system_conn = await asyncpg.connect(
                user=self.config.POSTGRES_USER,
                password=self.config.POSTGRES_PASSWORD,
                host=self.config.POSTGRES_HOST,
                port=self.config.POSTGRES_PORT,
                database='postgres'
            )

            exists = await system_conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.config.POSTGRES_DB
            )

            if not exists:
                await system_conn.execute(
                    f'CREATE DATABASE "{self.config.POSTGRES_DB}"'
                )
                self.logger.info(f"Database {self.config.POSTGRES_DB} created successfully")
            else:
                self.logger.info(f"Database {self.config.POSTGRES_DB} already exists")

            await system_conn.close()
        except asyncpg.exceptions.DuplicateDatabaseError:
            self.logger.info(f"Database {self.config.POSTGRES_DB} already exists")
        except Exception as e:
            self.logger.error(f"Failed to create database: {str(e)}")
            raise

    async def create_tables(self):
        """Create tables from all SQL files in migrations folder"""
        try:
            migrations_path = os.path.join(os.path.dirname(__file__), '..', 'migrations')
            
            # Get all .sql files and sort them
            sql_files = []
            for file in os.listdir(migrations_path):
                if file.endswith('.sql'):
                    sql_files.append(file)
            sql_files.sort()

            # Execute each SQL file in order
            async with self.transaction() as conn:
                for sql_file in sql_files:
                    file_path = os.path.join(migrations_path, sql_file)
                    with open(file_path, 'r') as f:
                        sql = f.read()
                    await conn.execute(sql)
                    self.logger.info(f"Executed migration file: {sql_file}")
                    
            self.logger.info("All database migrations completed successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute migrations: {str(e)}")
            raise

    async def initialize(self):
        """Initialize database and connection pool"""
        try:
            # Tạo database
            await self.create_database()
            
            # Thêm delay nhỏ để đảm bảo database đã được tạo xong
            await asyncio.sleep(1)  
            
            async def init(conn):
                await conn.set_type_codec(
                    'jsonb',
                    encoder=json.dumps,
                    decoder=json.loads,
                    schema='pg_catalog'
                )
                await conn.set_type_codec(
                    'json',
                    encoder=json.dumps,
                    decoder=json.loads,
                    schema='pg_catalog'
                )

            # Thử kết nối vài lần nếu fail
            max_retries = 3
            current_retry = 0
            while current_retry < max_retries:
                try:
                    self.pool = await asyncpg.create_pool(
                        user=self.config.POSTGRES_USER,
                        password=self.config.POSTGRES_PASSWORD,
                        database=self.config.POSTGRES_DB,
                        host=self.config.POSTGRES_HOST,
                        port=self.config.POSTGRES_PORT,
                        min_size=5,
                        max_size=20,
                        init=init
                    )
                    break  # Thoát loop nếu tạo pool thành công
                except asyncpg.InvalidCatalogNameError:
                    current_retry += 1
                    if current_retry == max_retries:
                        raise
                    await asyncio.sleep(1)  # Đợi 1s trước khi thử lại
            
            # Tạo tables sau khi đã có pool
            await self.create_tables()
            self.logger.info("Database initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {str(e)}")
            raise

    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            self.logger.info("Database connection pool closed")

    async def drop_database(self) -> None:
        """
        Drop the current database.
        WARNING: This will completely delete the current database. Use with extreme caution!
        """
        try:
            if not self.pool:
                raise Exception("Database pool not initialized")

            # First close our pool to release all its connections
            await self.pool.close()
            self.pool = None

            # Connect to postgres to drop the target database
            system_conn = await asyncpg.connect(
                user=self.config.POSTGRES_USER,
                password=self.config.POSTGRES_PASSWORD,
                host=self.config.POSTGRES_HOST,
                port=self.config.POSTGRES_PORT,
                database='postgres'
            )

            # Force kill all connections to target database
            await system_conn.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity 
                WHERE datname = $1
                AND pid <> pg_backend_pid()
            """, self.config.POSTGRES_DB)

            # Small delay to ensure connections are terminated
            await asyncio.sleep(1)

            # Now drop the database
            await system_conn.execute(f'DROP DATABASE IF EXISTS "{self.config.POSTGRES_DB}"')
            await system_conn.close()

            self.logger.info(f"Successfully dropped database: {self.config.POSTGRES_DB}")

        except Exception as e:
            self.logger.error(f"Error dropping database: {str(e)}")
            raise
          
    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    def _prepare_query_args(self, args: tuple) -> tuple:
        """Convert any dict arguments to JSON strings"""
        return tuple(
            json.dumps(arg) if isinstance(arg, dict) else arg
            for arg in args
        )

    async def fetchrow(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Fetch single row from database"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        async with self.pool.acquire() as conn:
            prepared_args = self._prepare_query_args(args)
            result = await conn.fetchrow(query, *prepared_args)
            return dict(result) if result else None

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch single value from database"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        async with self.pool.acquire() as conn:
            prepared_args = self._prepare_query_args(args)
            return await conn.fetchval(query, *prepared_args)

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Fetch multiple rows from database"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        async with self.pool.acquire() as conn:
            prepared_args = self._prepare_query_args(args)
            results = await conn.fetch(query, *prepared_args)
            return [dict(row) for row in results]

    async def execute(self, query: str, *args) -> str:
        """Execute database query"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        async with self.pool.acquire() as conn:
            prepared_args = self._prepare_query_args(args)
            return await conn.execute(query, *prepared_args)

    async def execute_many(self, query: str, args_list: List[tuple]) -> None:
        """Execute same query with multiple sets of arguments"""
        if not self.pool:
            raise Exception("Database pool not initialized")
            
        prepared_args_list = [self._prepare_query_args(args) for args in args_list]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, prepared_args_list)

    async def health_check(self) -> bool:
        """Check database connection health"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute('SELECT 1')
            return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {str(e)}")
            return False