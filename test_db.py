import asyncio
from src.config import async_session
from sqlalchemy import text

async def test():
    async with async_session() as session:
        await session.execute(text("SELECT 1"))
        print("Success")

asyncio.run(test())
