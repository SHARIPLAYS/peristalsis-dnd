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

CATEGORIES = ("clothing", "weapon", "consumables", "artifact", "other")


def safe_category(cat):
    return cat if cat in CATEGORIES else "other"


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

# --- ГЛОБАЛЬНЫЕ КОЛЛЕКЦИИ ---
base_inventory = []           # Шкафчик базы
item_templates = []           # Заготовки хоста
pending_trades = []           # Активные обмены


def new_id():
    return uuid.uuid4().hex[:12]


def add_item_to_bucket(bucket, item, count=None):
    """Складывает item в bucket с учётом стакинга (name+desc+category)."""
    if count is None:
        count = int(item.get("count", 1) or 1)
    count = max(1, int(count))
    name = item.get("name", "") or ""
    desc = item.get("desc", "") or ""
    cat = safe_category(item.get("category", "other"))

    for existing in bucket:
        if (existing.get("name") == name and
                (existing.get("desc") or "") == desc and
                safe_category(existing.get("category", "other")) == cat):
            existing["count"] = int(existing.get("count", 1) or 1) + count
            return existing

    new_item = {
        "id": new_id(),
        "name": name,
        "desc": desc,
        "category": cat,
        "count": count
    }
    bucket.append(new_item)
    return new_item


def find_item(lst, item_id):
    if not isinstance(lst, list):
        return None
    for i in lst:
        if str(i.get("id")) == str(item_id):
            return i
    return None


def take_from_stack(lst, item_id, count):
    """Забирает count из стека. Возвращает (snapshot, actual_count) или None."""
    item = find_item(lst, item_id)
    if not item:
        return None
    avail = int(item.get("count", 1) or 1)
    if avail <= 0:
        return None
    try:
        count = int(count)
    except (TypeError, ValueError):
        return None
    if count <= 0:
        return None
    take = min(count, avail)
    item["count"] = avail - take
    snapshot = {
        "name": item.get("name", "") or "",
        "desc": item.get("desc", "") or "",
        "category": safe_category(item.get("category", "other")),
    }
    if item["count"] <= 0:
        try:
            lst.remove(item)
        except ValueError:
            pass
    return snapshot, take


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
                "baseInventory": base_inventory,
                "itemTemplates": item_templates,
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
            "baseInventory": base_inventory,
            "itemTemplates": item_templates,
            "pendingTrades": pending_trades
        }))

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
                websockets.broadcast(clients, json.dumps({"type": "sync", "state": clock_state}))

            # --- РЕГИСТРАЦИЯ ---
            elif msg_type == "register_player":
                name = data.get("name")
                if name:
                    if name not in players_data:
                        players_data[name] = get_default_player()
                        await broadcast_players()
                    else:
                        ensure_player_shape(name)

            # --- СТАТЫ ---
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

            # --- ВЫДАЧА ПРЕДМЕТА ИГРОКУ ---
            elif msg_type == "update_player_data":
                target = data.get("target")
                data_type = data.get("dataType")
                item = data.get("item")
                if target and data_type and item:
                    ensure_player_shape(target)
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        if data_type == "inventory":
                            add_item_to_bucket(bucket, item)
                        else:
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
                            i for i in bucket if str(i.get("id")) != str(item_id)
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
                            if str(bucket[i].get("id")) == str(new_item.get("id")):
                                # Сохраняем id, заменяем остальное
                                merged = dict(new_item)
                                merged["id"] = bucket[i].get("id")
                                if data_type == "inventory":
                                    merged["category"] = safe_category(merged.get("category", "other"))
                                    merged["count"] = max(1, int(merged.get("count", 1) or 1))
                                bucket[i] = merged
                                break
                        await broadcast_players()

            # ================================================
            # ПЕРЕДАЧА
            # ================================================
            elif msg_type == "transfer_item":
                src = data.get("from")
                dst = data.get("to")
                data_type = data.get("dataType", "inventory")
                item_id = data.get("itemId")
                count = data.get("count", 1)
                if src and dst and item_id:
                    ensure_player_shape(src)
                    ensure_player_shape(dst)
                    bucket = players_data[src].get(data_type, [])
                    res = take_from_stack(bucket, item_id, count)
                    if res:
                        snapshot, taken = res
                        if data_type == "inventory":
                            add_item_to_bucket(players_data[dst].setdefault(data_type, []), snapshot, taken)
                        else:
                            players_data[dst].setdefault(data_type, []).append(snapshot)
                        await broadcast_players()
                        await broadcast_event("transfer", {
                            "from": src, "to": dst, "item": snapshot,
                            "count": taken, "dataType": data_type
                        })

            # ================================================
            # ШКАФЧИК (базовый инвентарь)
            # ================================================
            elif msg_type == "base_add":
                item = data.get("item")
                if item:
                    add_item_to_bucket(base_inventory, item)
                    await broadcast_social()
                    await broadcast_event("base_added", {"item": item})

            elif msg_type == "base_edit":
                new_item = data.get("item")
                if new_item:
                    existing = find_item(base_inventory, new_item.get("id"))
                    if existing:
                        existing["name"] = new_item.get("name", existing.get("name"))
                        existing["desc"] = new_item.get("desc", existing.get("desc") or "")
                        existing["category"] = safe_category(new_item.get("category", existing.get("category", "other")))
                        try:
                            existing["count"] = max(1, int(new_item.get("count", existing.get("count", 1))))
                        except (TypeError, ValueError):
                            pass
                        await broadcast_social()

            elif msg_type == "base_delete":
                item_id = data.get("itemId")
                if item_id:
                    for idx, it in enumerate(base_inventory):
                        if str(it.get("id")) == str(item_id):
                            base_inventory.pop(idx)
                            break
                    await broadcast_social()

            elif msg_type == "base_put":
                src = data.get("from")
                item_id = data.get("itemId")
                count = data.get("count", 1)
                if src and item_id:
                    ensure_player_shape(src)
                    bucket = players_data[src].get("inventory", [])
                    res = take_from_stack(bucket, item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(base_inventory, snapshot, taken)
                        await broadcast_players()
                        await broadcast_social()
                        await broadcast_event("base_put", {
                            "from": src, "item": snapshot, "count": taken
                        })

            elif msg_type == "base_take":
                player = data.get("player")
                base_item_id = data.get("baseItemId")
                count = data.get("count", 1)
                if player and base_item_id:
                    ensure_player_shape(player)
                    res = take_from_stack(base_inventory, base_item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(players_data[player].setdefault("inventory", []), snapshot, taken)
                        await broadcast_players()
                        await broadcast_social()
                        await broadcast_event("base_take", {
                            "player": player, "item": snapshot, "count": taken
                        })

            # ================================================
            # ЗАГОТОВКИ (templates)
            # ================================================
            elif msg_type == "template_add":
                item = data.get("item")
                if item:
                    add_item_to_bucket(item_templates, item)
                    await broadcast_social()

            elif msg_type == "template_edit":
                new_item = data.get("item")
                if new_item:
                    existing = find_item(item_templates, new_item.get("id"))
                    if existing:
                        existing["name"] = new_item.get("name", existing.get("name"))
                        existing["desc"] = new_item.get("desc", existing.get("desc") or "")
                        existing["category"] = safe_category(new_item.get("category", existing.get("category", "other")))
                        try:
                            existing["count"] = max(1, int(new_item.get("count", existing.get("count", 1))))
                        except (TypeError, ValueError):
                            pass
                        await broadcast_social()

            elif msg_type == "template_delete":
                item_id = data.get("itemId")
                if item_id:
                    for idx, it in enumerate(item_templates):
                        if str(it.get("id")) == str(item_id):
                            item_templates.pop(idx)
                            break
                    await broadcast_social()

            elif msg_type == "template_give":
                template_id = data.get("templateId")
                target = data.get("target")
                count = data.get("count", None)
                if template_id and target:
                    tpl = find_item(item_templates, template_id)
                    if tpl:
                        ensure_player_shape(target)
                        snapshot = {
                            "name": tpl.get("name", ""),
                            "desc": tpl.get("desc", "") or "",
                            "category": safe_category(tpl.get("category", "other")),
                        }
                        give_count = int(count) if count is not None else int(tpl.get("count", 1) or 1)
                        give_count = max(1, give_count)
                        add_item_to_bucket(
                            players_data[target].setdefault("inventory", []),
                            snapshot, give_count
                        )
                        await broadcast_players()
                        await broadcast_event("template_give", {
                            "target": target, "item": snapshot, "count": give_count
                        })

            # ================================================
            # ТРЕЙД
            # ================================================
            elif msg_type == "trade_propose":
                src = data.get("from")
                dst = data.get("to")
                from_item_id = data.get("fromItemId")
                to_item_id = data.get("toItemId")
                from_count = data.get("fromCount", 1)
                to_count = data.get("toCount", 1)
                if src and dst and from_item_id and to_item_id:
                    ensure_player_shape(src)
                    ensure_player_shape(dst)
                    f_item = find_item(players_data[src].get("inventory", []), from_item_id)
                    t_item = find_item(players_data[dst].get("inventory", []), to_item_id)
                    if f_item and t_item:
                        f_avail = int(f_item.get("count", 1) or 1)
                        t_avail = int(t_item.get("count", 1) or 1)
                        try:
                            fc = max(1, min(int(from_count), f_avail))
                            tc = max(1, min(int(to_count), t_avail))
                        except (TypeError, ValueError):
                            fc, tc = 1, 1
                        trade = {
                            "id": new_id(),
                            "from": src, "to": dst,
                            "fromItemId": from_item_id,
                            "fromCount": fc,
                            "fromItem": {
                                "name": f_item.get("name", ""),
                                "desc": f_item.get("desc", "") or "",
                                "category": safe_category(f_item.get("category", "other"))
                            },
                            "toItemId": to_item_id,
                            "toCount": tc,
                            "toItem": {
                                "name": t_item.get("name", ""),
                                "desc": t_item.get("desc", "") or "",
                                "category": safe_category(t_item.get("category", "other"))
                            },
                            "fromConfirmed": True,
                            "toConfirmed": False,
                            "createdAt": time.time()
                        }
                        pending_trades.append(trade)
                        await broadcast_social()
                        await broadcast_event("trade_proposed", trade)
                    else:
                        await broadcast_event("trade_failed", {"reason": "missing_item"})

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
                        f_res = take_from_stack(
                            players_data[src].get("inventory", []),
                            trade["fromItemId"], trade["fromCount"]
                        )
                        t_res = take_from_stack(
                            players_data[dst].get("inventory", []),
                            trade["toItemId"], trade["toCount"]
                        )
                        if f_res and t_res:
                            f_snap, f_taken = f_res
                            t_snap, t_taken = t_res
                            add_item_to_bucket(players_data[dst].setdefault("inventory", []), f_snap, f_taken)
                            add_item_to_bucket(players_data[src].setdefault("inventory", []), t_snap, t_taken)
                            pending_trades.remove(trade)
                            await broadcast_players()
                            await broadcast_social()
                            await broadcast_event("trade_completed", {
                                "from": src, "to": dst,
                                "fromItem": f_snap, "fromCount": f_taken,
                                "toItem": t_snap, "toCount": t_taken
                            })
                        else:
                            pending_trades.remove(trade)
                            await broadcast_social()
                            await broadcast_event("trade_failed", {"tradeId": trade_id, "reason": "missing_item"})
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
