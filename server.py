import asyncio
import websockets
import json
import time
import os

# Глобальное состояние часов
# baseTime: время в секундах (например, 12:00 = 43200)
clock_state = {
    "isRunning": False,
    "baseTime": 43200, 
    "lastUpdateTimestamp": time.time()
}
clients = set()

async def handler(websocket):
    clients.add(websocket)
    try:
        # При подключении нового игрока сразу отправляем ему актуальное время
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        
        async for message in websocket:
            data = json.loads(message)
            if data.get("type") == "update":
                # Хост прислал новые данные (запуск, пауза или перемотка)
                clock_state.update(data["state"])
                clock_state["lastUpdateTimestamp"] = time.time()
                
                # Мгновенно рассылаем обновленное состояние всем игрокам
                if clients:
                    websockets.broadcast(clients, json.dumps({"type": "sync", "state": clock_state}))
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.remove(websocket)

async def main():
    # Render автоматически задает переменную окружения PORT
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Server started on port {port}")
        await asyncio.Future()  # Работаем бесконечно

if __name__ == "__main__":
    asyncio.run(main())