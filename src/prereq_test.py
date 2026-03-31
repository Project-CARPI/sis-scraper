import asyncio
import json

import aiohttp

import sis_api


async def main():
    async with aiohttp.ClientSession() as session:
        # 75223 for basics
        # 72027 for nested parenthesis

        with open("temp.json", "r") as file:
            data = json.load(file)

        all_data = data["data"]
        all_crns = set()
        for item in all_data:
            all_crns.add(item["courseReferenceNumber"])

        # for crn in all_crns:
        result = await sis_api.get_class_prerequisites(session, "202509", "72027")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
