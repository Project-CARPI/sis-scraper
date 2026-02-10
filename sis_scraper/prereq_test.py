import asyncio
import aiohttp
import sis_api

async def main():
    async with aiohttp.ClientSession() as session:
        # 75223 for basics
        # 72027 for nested parenthesis
        await sis_api.get_class_prerequisites(session, "202509", "72027")

if __name__ == "__main__":
    asyncio.run(main())