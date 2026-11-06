import asyncio
from datetime import datetime, timedelta

import websockets

HOST = "127.0.0.1"
PORT = 10050


async def handler(websocket):
    async for message in websocket:
        print(f"Получено от A: {message}")
        try:
            dt = datetime.strptime(message, "%Y-%m-%d %H:%M")
            result = dt - timedelta(days=1)
            reply = result.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            reply = f"Ошибка: не удалось разобрать дату '{message}'"

        await websocket.send(reply)
        print(f"Отправлено в A: {reply}")


async def main():
    async with websockets.serve(handler, HOST, PORT):
        print(f"Приложение B запущено: ws://{HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
