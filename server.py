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

# НОВОЕ: Глобальное состояние инвентаря и статусов игроков
players_data = {}

clients = set()

async def handler(websocket):
    clients.add(websocket)
    try:
        # При подключении нового игрока сразу отправляем ему актуальное время
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        
        # НОВОЕ: Сразу отправляем данные об инвентаре всех игроков
        await websocket.send(json.dumps({"type": "sync_players", "playersData": players_data}))
        
        async for message in websocket:
            data = json.loads(message)
            
            # --- ЛОГИКА ЧАСОВ ---
            if data.get("type") == "update":
                clock_state.update(data["state"])
                clock_state["lastUpdateTimestamp"] = time.time()
                if clients:
                    websockets.broadcast(clients, json.dumps({"type": "sync", "state": clock_state}))
            
            # --- РЕГИСТРАЦИЯ ИГРОКА ---
            elif data.get("type") == "register_player":
                player_name = data.get("name")
                if player_name and player_name not in players_data:
                    players_data[player_name] = {"anxiety": [], "inventory": []}
                    if clients:
                        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

            # --- ВЫДАЧА ПРЕДМЕТА ---
            elif data.get("type") == "update_player_data":
                target = data.get("target")
                data_type = data.get("dataType")
                item = data.get("item")
                if target and data_type and item:
                    if target not in players_data:
                        players_data[target] = {"anxiety": [], "inventory": []}
                    if data_type in players_data[target]:
                        players_data[target][data_type].append(item)
                    if clients:
                        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

            # --- УДАЛЕНИЕ ПРЕДМЕТА ---
            elif data.get("type") == "delete_item":
                target = data.get("target")
                data_type = data.get("dataType")
                item_id = data.get("itemId")
                if target in players_data and data_type in players_data[target]:
                    # Оставляем только те предметы, ID которых не совпадает с удаляемым
                    players_data[target][data_type] = [i for i in players_data[target][data_type] if i.get("id") != item_id]
                    if clients:
                        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

            # --- РЕДАКТИРОВАНИЕ ПРЕДМЕТА ---
            elif data.get("type") == "edit_item":
                target = data.get("target")
                data_type = data.get("dataType")
                new_item = data.get("item")
                if target in players_data and data_type in players_data[target]:
                    for i in range(len(players_data[target][data_type])):
                        if players_data[target][data_type][i].get("id") == new_item.get("id"):
                            players_data[target][data_type][i] = new_item
                            break
                    if clients:
                        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

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
