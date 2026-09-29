import asyncio
import websockets
import json
import time
import os

# Глобальное состояние часов
clock_state = {
    "isRunning": False,
    "baseTime": 43200, 
    "lastUpdateTimestamp": time.time()
}

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ТЕЛА ---
# Вынесена наверх, чтобы не ломать логику if-elif
def get_default_body():
    return {
        "head": {"hp": 8, "maxHp": 8, "def": 0, "name": "Голова"},
        "torso": {"hp": 30, "maxHp": 30, "def": 0, "name": "Торс"},
        "leftArm": {"hp": 10, "maxHp": 10, "def": 0, "name": "Левая рука"},
        "rightArm": {"hp": 10, "maxHp": 10, "def": 0, "name": "Правая рука"},
        "leftLeg": {"hp": 15, "maxHp": 15, "def": 0, "name": "Левая нога"},
        "rightLeg": {"hp": 15, "maxHp": 15, "def": 0, "name": "Правая нога"}
    }

# Глобальное состояние инвентаря и статусов игроков
players_data = {}
clients = set()

async def handler(websocket):
    clients.add(websocket)
    try:
        # При подключении нового игрока сразу отправляем ему актуальное время и данные
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
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
                    players_data[player_name] = {"anxiety": [], "inventory": [], "body": get_default_body()}
                    if clients:
                        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

            # --- ИЗМЕНЕНИЕ ТЕЛА (ХП/ЗАЩИТА) ---
            elif data.get("type") == "update_body":
                target = data.get("target")
                part_id = data.get("part")
                new_hp = data.get("hp")
                new_def = data.get("def")
                
                if target in players_data:
                    if "body" not in players_data[target]:
                        players_data[target]["body"] = get_default_body()
                        
                    if part_id in players_data[target]["body"]:
                        players_data[target]["body"][part_id]["hp"] = new_hp
                        players_data[target]["body"][part_id]["def"] = new_def
                        if clients:
                            websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

            # --- ВЫДАЧА ПРЕДМЕТА ---
            elif data.get("type") == "update_player_data":
                target = data.get("target")
                data_type = data.get("dataType")
                item = data.get("item")
                if target and data_type and item:
                    if target not in players_data:
                        players_data[target] = {"anxiety": [], "inventory": [], "body": get_default_body()}
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
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Server started on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
