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


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_default_body():
    return {
        "head":     {"hp": 8,  "maxHp": 8,  "def": 0, "name": "Голова"},
        "torso":    {"hp": 30, "maxHp": 30, "def": 0, "name": "Торс"},
        "leftArm":  {"hp": 10, "maxHp": 10, "def": 0, "name": "Левая рука"},
        "rightArm": {"hp": 10, "maxHp": 10, "def": 0, "name": "Правая рука"},
        "leftLeg":  {"hp": 15, "maxHp": 15, "def": 0, "name": "Левая нога"},
        "rightLeg": {"hp": 15, "maxHp": 15, "def": 0, "name": "Правая нога"}
    }


def get_default_player():
    return {
        "anxiety": [],          # список состояний / дебаффов
        "inventory": [],        # список экипировки
        "body": get_default_body(),
        "morale": 10,           # 0..10
        "anxietyLevel": 0.0     # 0..4, может быть дробным
    }


def ensure_player_shape(name):
    """Гарантирует, что у игрока есть все поля (для старых сохранений)."""
    if name not in players_data:
        players_data[name] = get_default_player()
        return
    p = players_data[name]
    if "anxiety" not in p or not isinstance(p["anxiety"], list):
        p["anxiety"] = []
    if "inventory" not in p or not isinstance(p["inventory"], list):
        p["inventory"] = []
    if "body" not in p or not isinstance(p["body"], dict):
        p["body"] = get_default_body()
    if "morale" not in p:
        p["morale"] = 10
    if "anxietyLevel" not in p:
        p["anxietyLevel"] = 0.0


players_data = {}
clients = set()


async def broadcast_players():
    if clients:
        websockets.broadcast(
            clients,
            json.dumps({"type": "sync_players", "playersData": players_data})
        )


async def handler(websocket):
    clients.add(websocket)
    try:
        # Сразу после подключения отправляем актуальное состояние
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        await websocket.send(json.dumps({"type": "sync_players", "playersData": players_data}))

        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            # --- ЧАСЫ ---
            if msg_type == "update":
                clock_state.update(data["state"])
                clock_state["lastUpdateTimestamp"] = time.time()
                if clients:
                    websockets.broadcast(
                        clients,
                        json.dumps({"type": "sync", "state": clock_state})
                    )

            # --- РЕГИСТРАЦИЯ ИГРОКА ---
            elif msg_type == "register_player":
                name = data.get("name")
                if name:
                    if name not in players_data:
                        players_data[name] = get_default_player()
                        await broadcast_players()
                    else:
                        ensure_player_shape(name)

            # --- ИЗМЕНЕНИЕ СТАТОВ (тело / мораль / тревога) ---
            elif msg_type == "update_body":
                target = data.get("target")
                part_id = data.get("part")
                new_hp = data.get("hp")
                new_def = data.get("def", 0)

                if target:
                    ensure_player_shape(target)
                    p = players_data[target]

                    if part_id == "morale":
                        # Мораль: дробное не запрещено, но фактически целое
                        try:
                            p["morale"] = max(0.0, min(10.0, float(new_hp)))
                        except (TypeError, ValueError):
                            pass

                    elif part_id == "anxietyLevel":
                        # Тревога: 0..4, десятичная
                        try:
                            p["anxietyLevel"] = max(0.0, min(4.0, float(new_hp)))
                        except (TypeError, ValueError):
                            pass

                    elif part_id in p["body"]:
                        try:
                            hp = int(new_hp)
                            df = int(new_def)
                        except (TypeError, ValueError):
                            hp, df = None, None
                        if hp is not None:
                            max_hp = p["body"][part_id].get("maxHp", hp)
                            p["body"][part_id]["hp"]  = max(0, min(max_hp, hp))
                            p["body"][part_id]["def"] = max(0, df)

                    await broadcast_players()

            # --- ВЫДАЧА ПРЕДМЕТА ---
            elif msg_type == "update_player_data":
                target = data.get("target")
                data_type = data.get("dataType")
                item = data.get("item")
                if target and data_type and item:
                    ensure_player_shape(target)
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        bucket.append(item)
                        await broadcast_players()

            # --- УДАЛЕНИЕ ПРЕДМЕТА ---
            elif msg_type == "delete_item":
                target = data.get("target")
                data_type = data.get("dataType")
                item_id = data.get("itemId")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        players_data[target][data_type] = [
                            i for i in bucket if i.get("id") != item_id
                        ]
                        await broadcast_players()

            # --- РЕДАКТИРОВАНИЕ ПРЕДМЕТА ---
            elif msg_type == "edit_item":
                target = data.get("target")
                data_type = data.get("dataType")
                new_item = data.get("item")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        for i in range(len(bucket)):
                            if bucket[i].get("id") == new_item.get("id"):
                                bucket[i] = new_item
                                break
                        await broadcast_players()

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Server started on port {port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
