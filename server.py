import asyncio
import websockets
import json
import time
import os
import uuid

clock_state = {
    "isRunning": False,
    "baseTime": 43200,
    "lastUpdateTimestamp": time.time()
}


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
        "anxiety": [],
        "inventory": [],
        "body": get_default_body(),
        "morale": 10,
        "anxietyLevel": 0.0
    }


def ensure_player_shape(name):
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

# --- СОЦИАЛЬНОЕ СОСТОЯНИЕ ---
shared_inventory = []
pending_trades = []


def new_id():
    return uuid.uuid4().hex[:12]


def find_item(lst, item_id):
    if not isinstance(lst, list):
        return None
    for i in lst:
        if str(i.get("id")) == str(item_id):
            return i
    return None


def remove_item(lst, item_id):
    if not isinstance(lst, list):
        return None
    for idx, i in enumerate(lst):
        if str(i.get("id")) == str(item_id):
            return lst.pop(idx)
    return None


async def broadcast_players():
    if clients:
        websockets.broadcast(
            clients,
            json.dumps({"type": "sync_players", "playersData": players_data})
        )


async def broadcast_social():
    if clients:
        websockets.broadcast(
            clients,
            json.dumps({
                "type": "sync_social",
                "sharedInventory": shared_inventory,
                "pendingTrades": pending_trades
            })
        )


async def broadcast_event(kind, data):
    if clients:
        websockets.broadcast(
            clients,
            json.dumps({"type": "event", "kind": kind, "data": data})
        )


async def handler(websocket):
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        await websocket.send(json.dumps({"type": "sync_players", "playersData": players_data}))
        await websocket.send(json.dumps({
            "type": "sync_social",
            "sharedInventory": shared_inventory,
            "pendingTrades": pending_trades
        }))

        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "update":
                clock_state.update(data["state"])
                clock_state["lastUpdateTimestamp"] = time.time()
                websockets.broadcast(clients, json.dumps({"type": "sync", "state": clock_state}))

            elif msg_type == "register_player":
                name = data.get("name")
                if name:
                    if name not in players_data:
                        players_data[name] = get_default_player()
                        await broadcast_players()
                    else:
                        ensure_player_shape(name)

            elif msg_type == "update_body":
                target = data.get("target")
                part_id = data.get("part")
                new_hp = data.get("hp")
                new_def = data.get("def", 0)
                new_max = data.get("maxHp")

                if target:
                    ensure_player_shape(target)
                    p = players_data[target]

                    if part_id == "morale":
                        try:
                            p["morale"] = max(0.0, float(new_hp))
                        except (TypeError, ValueError):
                            pass
                        if new_max is not None:
                            try:
                                p["maxMorale"] = max(0, int(new_max))
                            except (TypeError, ValueError):
                                pass

                    elif part_id == "anxietyLevel":
                        try:
                            p["anxietyLevel"] = max(0.0, float(new_hp))
                        except (TypeError, ValueError):
                            pass
                        if new_max is not None:
                            try:
                                p["maxAnxiety"] = max(0, int(new_max))
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
                            if new_max is not None:
                                try:
                                    max_hp = max(0, int(new_max))
                                    p["body"][part_id]["maxHp"] = max_hp
                                except (TypeError, ValueError):
                                    pass
                            p["body"][part_id]["hp"] = max(0, min(max_hp, hp))
                            p["body"][part_id]["def"] = max(0, df)

                    await broadcast_players()

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

            elif msg_type == "delete_item":
                target = data.get("target")
                data_type = data.get("dataType")
                item_id = data.get("itemId")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        players_data[target][data_type] = [
                            i for i in bucket if str(i.get("id")) != str(item_id)
                        ]
                        await broadcast_players()

            elif msg_type == "edit_item":
                target = data.get("target")
                data_type = data.get("dataType")
                new_item = data.get("item")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        for i in range(len(bucket)):
                            if str(bucket[i].get("id")) == str(new_item.get("id")):
                                bucket[i] = new_item
                                break
                        await broadcast_players()

            # ================================================
            # СОЦИАЛЬНЫЕ ВЗАИМОДЕЙСТВИЯ
            # ================================================

            elif msg_type == "transfer_item":
                src = data.get("from")
                dst = data.get("to")
                data_type = data.get("dataType", "inventory")
                item_id = data.get("itemId")
                if src and dst and item_id:
                    ensure_player_shape(src)
                    ensure_player_shape(dst)   # получатель может быть офлайн
                    bucket = players_data[src].get(data_type, [])
                    item = remove_item(bucket, item_id)
                    if item:
                        players_data[dst].setdefault(data_type, []).append(item)
                        await broadcast_players()
                        await broadcast_event("transfer", {
                            "from": src, "to": dst, "item": item, "dataType": data_type
                        })

            elif msg_type == "shared_put":
                src = data.get("from")
                item_id = data.get("itemId")
                data_type = data.get("dataType", "inventory")
                if src and item_id:
                    ensure_player_shape(src)
                    bucket = players_data[src].get(data_type, [])
                    item = remove_item(bucket, item_id)
                    if item:
                        entry = {
                            "id": new_id(),
                            "item": item,
                            "dataType": data_type,
                            "putBy": src,
                            "putAt": time.time()
                        }
                        shared_inventory.append(entry)
                        await broadcast_players()
                        await broadcast_social()
                        await broadcast_event("shared_put", {
                            "from": src, "item": item, "sharedId": entry["id"]
                        })

            elif msg_type == "shared_take":
                player = data.get("player")
                shared_id = data.get("sharedItemId")
                if player and shared_id:
                    ensure_player_shape(player)
                    entry = next((s for s in shared_inventory if s["id"] == shared_id), None)
                    if entry:
                        shared_inventory.remove(entry)
                        players_data[player].setdefault(entry["dataType"], []).append(entry["item"])
                        await broadcast_players()
                        await broadcast_social()
                        await broadcast_event("shared_take", {
                            "player": player, "item": entry["item"]
                        })

            elif msg_type == "trade_propose":
                src = data.get("from")
                dst = data.get("to")
                from_item_id = data.get("fromItemId")
                to_item_id = data.get("toItemId")
                if src and dst and from_item_id and to_item_id:
                    ensure_player_shape(src)
                    ensure_player_shape(dst)   # получатель может быть офлайн
                    from_item = find_item(players_data[src].get("inventory", []), from_item_id)
                    to_item = find_item(players_data[dst].get("inventory", []), to_item_id)
                    if from_item and to_item:
                        trade = {
                            "id": new_id(),
                            "from": src,
                            "to": dst,
                            "fromDataType": "inventory",
                            "fromItemId": from_item_id,
                            "fromItem": from_item,
                            "toDataType": "inventory",
                            "toItemId": to_item_id,
                            "toItem": to_item,
                            "fromConfirmed": True,
                            "toConfirmed": False,
                            "createdAt": time.time()
                        }
                        pending_trades.append(trade)
                        await broadcast_social()
                        await broadcast_event("trade_proposed", trade)
                    else:
                        await broadcast_event("trade_failed", {
                            "reason": "missing_item", "tradeId": None
                        })

            elif msg_type == "trade_confirm":
                trade_id = data.get("tradeId")
                player = data.get("player")
                trade = next((t for t in pending_trades if t["id"] == trade_id), None)
                if trade and player in (trade["from"], trade["to"]):
                    if player == trade["from"]:
                        trade["fromConfirmed"] = True
                    else:
                        trade["toConfirmed"] = True

                    if trade["fromConfirmed"] and trade["toConfirmed"]:
                        src = trade["from"]
                        dst = trade["to"]
                        f_item = remove_item(players_data[src].get(trade["fromDataType"], []), trade["fromItemId"])
                        t_item = remove_item(players_data[dst].get(trade["toDataType"], []), trade["toItemId"])
                        if f_item and t_item:
                            players_data[dst].setdefault(trade["fromDataType"], []).append(f_item)
                            players_data[src].setdefault(trade["toDataType"], []).append(t_item)
                            pending_trades.remove(trade)
                            await broadcast_players()
                            await broadcast_social()
                            await broadcast_event("trade_completed", {
                                "from": src, "to": dst,
                                "fromItem": f_item, "toItem": t_item
                            })
                        else:
                            pending_trades.remove(trade)
                            await broadcast_social()
                            await broadcast_event("trade_failed", {"tradeId": trade_id})
                    else:
                        await broadcast_social()

            elif msg_type == "trade_cancel":
                trade_id = data.get("tradeId")
                player = data.get("player")
                trade = next((t for t in pending_trades if t["id"] == trade_id), None)
                if trade and (not player or player in (trade["from"], trade["to"])):
                    pending_trades.remove(trade)
                    await broadcast_social()
                    await broadcast_event("trade_cancelled", {
                        "tradeId": trade_id, "by": player
                    })

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
