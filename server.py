import asyncio
import websockets
import json
import time
import os
import ssl
import uuid
import traceback
import redis.asyncio as redis

# ================================================================
#                       REDIS (UPSTASH)
# ================================================================
REDIS_URL = os.environ.get("REDIS_URL")
r = None  # будет инициализирован в main()

# ================================================================
#                       СОСТОЯНИЕ
# ================================================================
clock_state = {
    "isRunning": False,
    "baseTime": 43200,
    "lastUpdateTimestamp": time.time()
}

CATEGORIES = ("clothing", "weapon", "consumables", "artifact", "other")
POLITICS_KEYS = ("comintern", "moralintern", "neutral", "redFeathers", "utopia")

players_data = {}
clients = set()

base_inventory = []
base_royals = 0
item_templates = []
pending_trades = []

# Lock, чтобы автосейв и ручной сейв не пересекались
save_lock = asyncio.Lock()


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


def get_default_politics():
    return {k: 0 for k in POLITICS_KEYS}


def get_default_player():
    return {
        "anxiety": [],
        "inventory": [],
        "body": get_default_body(),
        "morale": 10,
        "anxietyLevel": 0.0,
        "royals": 0,
        "politics": get_default_politics()
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
    if "royals" not in p:
        p["royals"] = 0
    if "politics" not in p or not isinstance(p["politics"], dict):
        p["politics"] = get_default_politics()
    else:
        for k in POLITICS_KEYS:
            if k not in p["politics"]:
                p["politics"][k] = 0


# ================================================================
#                    СОХРАНЕНИЕ / ЗАГРУЗКА
# ================================================================
async def save_state():
    """Сохраняет всё состояние игры в Redis."""
    if not r:
        return
    async with save_lock:
        try:
            pipe = r.pipeline()
            pipe.set("dnd:clock_state", json.dumps(clock_state))
            pipe.set("dnd:players_data", json.dumps(players_data, ensure_ascii=False))
            pipe.set("dnd:base_inventory", json.dumps(base_inventory, ensure_ascii=False))
            pipe.set("dnd:base_royals", json.dumps(base_royals))
            pipe.set("dnd:item_templates", json.dumps(item_templates, ensure_ascii=False))
            pipe.set("dnd:pending_trades", json.dumps(pending_trades, ensure_ascii=False))
            await pipe.execute()
        except Exception as e:
            print(f"[save_state] Ошибка: {e}", flush=True)


async def load_state():
    """Загружает состояние из Redis при старте сервера."""
    global base_royals
    if not r:
        print("[load_state] REDIS_URL не задан, пропускаем загрузку", flush=True)
        return
    try:
        pipe = r.pipeline()
        pipe.get("dnd:clock_state")
        pipe.get("dnd:players_data")
        pipe.get("dnd:base_inventory")
        pipe.get("dnd:base_royals")
        pipe.get("dnd:item_templates")
        pipe.get("dnd:pending_trades")
        results = await pipe.execute()

        if results[0]:
            clock_state.update(json.loads(results[0]))

        if results[1]:
            players_data.clear()
            players_data.update(json.loads(results[1]))

        if results[2]:
            base_inventory.clear()
            base_inventory.extend(json.loads(results[2]))

        if results[3]:
            base_royals = json.loads(results[3])

        if results[4]:
            item_templates.clear()
            item_templates.extend(json.loads(results[4]))

        if results[5]:
            pending_trades.clear()
            pending_trades.extend(json.loads(results[5]))

        print(f"[load_state] Состояние загружено: "
              f"{len(players_data)} игроков, "
              f"{len(base_inventory)} предметов в шкафчике, "
              f"{len(item_templates)} заготовок", flush=True)
    except Exception as e:
        print(f"[load_state] Ошибка: {e}", flush=True)
        traceback.print_exc()


async def autosave_loop():
    """Фоновое автосохранение каждые 15 секунд."""
    while True:
        await asyncio.sleep(15)
        await save_state()


# ================================================================
#                       ХЕЛПЕРЫ
# ================================================================
def new_id():
    return uuid.uuid4().hex[:12]


def safe_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def add_item_to_bucket(bucket, item, count=None):
    if count is None:
        count = safe_int(item.get("count", 1), 1)
    count = max(1, count)
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
    item = find_item(lst, item_id)
    if not item:
        return None
    avail = int(item.get("count", 1) or 1)
    if avail <= 0:
        return None
    count = safe_int(count, 0)
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


# ================================================================
#                       BROADCAST
# ================================================================
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
                "baseRoyals": base_royals,
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


# ================================================================
#                       ОБРАБОТЧИК WS
# ================================================================
async def handler(websocket):
    global base_royals
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        await websocket.send(json.dumps({"type": "sync_players", "playersData": players_data}))
        await websocket.send(json.dumps({
            "type": "sync_social",
            "baseInventory": base_inventory,
            "baseRoyals": base_royals,
            "itemTemplates": item_templates,
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
                await save_state()

            elif msg_type == "register_player":
                name = data.get("name")
                if name:
                    if name not in players_data:
                        players_data[name] = get_default_player()
                        await broadcast_players()
                        await save_state()
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

            # --- РОЯЛЫ ИГРОКА ---
            elif msg_type == "update_royals":
                target = data.get("target")
                amount = data.get("amount")
                if target:
                    ensure_player_shape(target)
                    players_data[target]["royals"] = max(0, safe_int(amount, 0))
                    await broadcast_players()

            # --- РОЯЛЫ БАЗЫ ---
            elif msg_type == "base_royals_set":
                base_royals = max(0, safe_int(data.get("amount"), 0))
                await broadcast_social()

            # --- ПЕРЕДАЧА РОЯЛОВ ---
            elif msg_type == "transfer_royals":
                src = data.get("from")
                dst = data.get("to")
                amount = max(0, safe_int(data.get("amount"), 0))
                if src and dst and amount > 0:
                    ensure_player_shape(src)
                    ensure_player_shape(dst)
                    src_r = int(players_data[src].get("royals", 0) or 0)
                    if src_r >= amount:
                        players_data[src]["royals"] = src_r - amount
                        players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) + amount
                        await broadcast_players()
                        await broadcast_event("royals_transfer", {
                            "from": src, "to": dst, "amount": amount
                        })

            # --- ПОЛИТИЧЕСКИЕ ХАРАКТЕРИСТИКИ ---
            elif msg_type == "update_politics":
                target = data.get("target")
                key = data.get("key")
                if target and key in POLITICS_KEYS:
                    ensure_player_shape(target)
                    cur = int(players_data[target]["politics"].get(key, 0) or 0)
                    if "delta" in data:
                        try:
                            delta = int(data["delta"])
                        except (TypeError, ValueError):
                            delta = 0
                        new_val = cur + delta
                    elif "value" in data:
                        new_val = safe_int(data["value"], cur)
                    else:
                        new_val = cur
                    players_data[target]["politics"][key] = new_val
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
                                merged = dict(new_item)
                                merged["id"] = bucket[i].get("id")
                                if data_type == "inventory":
                                    merged["category"] = safe_category(merged.get("category", "other"))
                                    merged["count"] = max(1, safe_int(merged.get("count", 1), 1))
                                bucket[i] = merged
                                break
                        await broadcast_players()

            # ================================================
            # ПЕРЕДАЧА ПРЕДМЕТОВ
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
            # ШКАФЧИК
            # ================================================
            elif msg_type == "base_add":
                item = data.get("item")
                if item:
                    add_item_to_bucket(base_inventory, item)
                    await broadcast_social()

            elif msg_type == "base_edit":
                new_item = data.get("item")
                if new_item:
                    existing = find_item(base_inventory, new_item.get("id"))
                    if existing:
                        existing["name"] = new_item.get("name", existing.get("name"))
                        existing["desc"] = new_item.get("desc", existing.get("desc") or "")
                        existing["category"] = safe_category(new_item.get("category", existing.get("category", "other")))
                        existing["count"] = max(1, safe_int(new_item.get("count", existing.get("count", 1)), 1))
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

            elif msg_type == "base_royals_put":
                player = data.get("player")
                amount = max(0, safe_int(data.get("amount"), 0))
                if player and amount > 0:
                    ensure_player_shape(player)
                    have = int(players_data[player].get("royals", 0) or 0)
                    if have >= amount:
                        players_data[player]["royals"] = have - amount
                        base_royals += amount
                        await broadcast_players()
                        await broadcast_social()
                        await broadcast_event("base_royals_put", {
                            "player": player, "amount": amount
                        })

            elif msg_type == "base_royals_take":
                player = data.get("player")
                amount = max(0, safe_int(data.get("amount"), 0))
                if player and amount > 0 and base_royals >= amount:
                    ensure_player_shape(player)
                    base_royals -= amount
                    players_data[player]["royals"] = int(players_data[player].get("royals", 0) or 0) + amount
                    await broadcast_players()
                    await broadcast_social()
                    await broadcast_event("base_royals_take", {
                        "player": player, "amount": amount
                    })

            # ================================================
            # ЗАГОТОВКИ
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
                        existing["count"] = max(1, safe_int(new_item.get("count", existing.get("count", 1)), 1))
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
                        give_count = safe_int(count, safe_int(tpl.get("count", 1), 1)) if count is not None else safe_int(tpl.get("count", 1), 1)
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
                from_type = data.get("fromType", "item")
                to_type = data.get("toType", "item")
                if not src or not dst:
                    continue
                ensure_player_shape(src)
                ensure_player_shape(dst)

                from_item_id = data.get("fromItemId")
                from_count = max(1, safe_int(data.get("fromCount", 1), 1))
                from_royals = max(0, safe_int(data.get("fromRoyals", 0), 0))

                to_item_id = data.get("toItemId")
                to_count = max(1, safe_int(data.get("toCount", 1), 1))
                to_royals = max(0, safe_int(data.get("toRoyals", 0), 0))

                f_ok, f_snap = False, None
                if from_type == "royals":
                    if int(players_data[src].get("royals", 0) or 0) >= from_royals and from_royals > 0:
                        f_ok = True
                        f_snap = {"type": "royals", "amount": from_royals}
                else:
                    f_item = find_item(players_data[src].get("inventory", []), from_item_id)
                    if f_item:
                        f_avail = int(f_item.get("count", 1) or 1)
                        from_count = min(from_count, f_avail)
                        f_ok = True
                        f_snap = {
                            "type": "item",
                            "itemId": from_item_id,
                            "count": from_count,
                            "item": {
                                "name": f_item.get("name", ""),
                                "desc": f_item.get("desc", "") or "",
                                "category": safe_category(f_item.get("category", "other"))
                            }
                        }

                t_ok, t_snap = False, None
                if to_type == "royals":
                    if int(players_data[dst].get("royals", 0) or 0) >= to_royals and to_royals > 0:
                        t_ok = True
                        t_snap = {"type": "royals", "amount": to_royals}
                else:
                    t_item = find_item(players_data[dst].get("inventory", []), to_item_id)
                    if t_item:
                        t_avail = int(t_item.get("count", 1) or 1)
                        to_count = min(to_count, t_avail)
                        t_ok = True
                        t_snap = {
                            "type": "item",
                            "itemId": to_item_id,
                            "count": to_count,
                            "item": {
                                "name": t_item.get("name", ""),
                                "desc": t_item.get("desc", "") or "",
                                "category": safe_category(t_item.get("category", "other"))
                            }
                        }

                if f_ok and t_ok:
                    trade = {
                        "id": new_id(),
                        "from": src, "to": dst,
                        "fromType": from_type,
                        "fromRoyals": from_royals if from_type == "royals" else 0,
                        "fromItemId": from_item_id if from_type == "item" else None,
                        "fromCount": from_count if from_type == "item" else 0,
                        "fromItem": (f_snap.get("item") if f_snap and from_type == "item" else None),
                        "toType": to_type,
                        "toRoyals": to_royals if to_type == "royals" else 0,
                        "toItemId": to_item_id if to_type == "item" else None,
                        "toCount": to_count if to_type == "item" else 0,
                        "toItem": (t_snap.get("item") if t_snap and to_type == "item" else None),
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

                        ok = True
                        f_side = None
                        t_side = None

                        if trade["fromType"] == "royals":
                            have = int(players_data[src].get("royals", 0) or 0)
                            if have < trade["fromRoyals"]:
                                ok = False
                            else:
                                f_side = {"type": "royals", "amount": trade["fromRoyals"]}
                        else:
                            f_item = take_from_stack(
                                players_data[src].get("inventory", []),
                                trade["fromItemId"], trade["fromCount"]
                            )
                            if not f_item:
                                ok = False
                            else:
                                f_snap, f_taken = f_item
                                f_side = {"type": "item", "item": f_snap, "count": f_taken}

                        if ok:
                            if trade["toType"] == "royals":
                                have = int(players_data[dst].get("royals", 0) or 0)
                                if have < trade["toRoyals"]:
                                    ok = False
                                else:
                                    t_side = {"type": "royals", "amount": trade["toRoyals"]}
                            else:
                                t_item = take_from_stack(
                                    players_data[dst].get("inventory", []),
                                    trade["toItemId"], trade["toCount"]
                                )
                                if not t_item:
                                    ok = False
                                else:
                                    t_snap, t_taken = t_item
                                    t_side = {"type": "item", "item": t_snap, "count": t_taken}

                        if not ok:
                            if f_side and f_side["type"] == "item":
                                add_item_to_bucket(
                                    players_data[src].setdefault("inventory", []),
                                    f_side["item"], f_side["count"]
                                )
                            pending_trades.remove(trade)
                            await broadcast_players()
                            await broadcast_social()
                            await broadcast_event("trade_failed", {"tradeId": trade_id})
                        else:
                            if f_side["type"] == "royals":
                                players_data[src]["royals"] = int(players_data[src].get("royals", 0) or 0) - f_side["amount"]
                                players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) + f_side["amount"]
                            else:
                                add_item_to_bucket(
                                    players_data[dst].setdefault("inventory", []),
                                    f_side["item"], f_side["count"]
                                )
                            if t_side["type"] == "royals":
                                players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) - t_side["amount"]
                                players_data[src]["royals"] = int(players_data[src].get("royals", 0) or 0) + t_side["amount"]
                            else:
                                add_item_to_bucket(
                                    players_data[src].setdefault("inventory", []),
                                    t_side["item"], t_side["count"]
                                )
                            pending_trades.remove(trade)
                            await broadcast_players()
                            await broadcast_social()
                            await broadcast_event("trade_completed", {
                                "from": src, "to": dst,
                                "fromSide": f_side, "toSide": t_side
                            })
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


# ================================================================
#                       ЗАПУСК
# ================================================================
async def main():
    global r
    port = int(os.environ.get("PORT", 8765))

    print("[main] ========== STARTUP ==========", flush=True)
    print(f"[main] PORT={port}", flush=True)
    print(f"[main] REDIS_URL is set: {bool(REDIS_URL)}", flush=True)

    if REDIS_URL:
        url_clean = REDIS_URL.strip()
        try:
            host_part = url_clean.split('@')[-1]
            print(f"[main] REDIS_URL host part: {host_part}", flush=True)
            print(f"[main] REDIS_URL scheme: {url_clean[:8]}...", flush=True)
        except Exception:
            pass

        try:
            print("[main] Подключаемся к Redis с явным SSL...", flush=True)

            ssl_params = {}
            if url_clean.startswith("rediss://"):
                ssl_params = {
                    "ssl": True,
                    "ssl_cert_reqs": ssl.CERT_NONE,
                }
                print("[main] Обнаружен rediss:// — включаем SSL", flush=True)

            r = redis.from_url(
                url_clean,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=30,
                socket_connect_timeout=10,
                **ssl_params,
            )

            pong = await r.ping()
            print(f"[main] Redis ping ответил: {pong}", flush=True)

            test_val = f"startup_ok_{int(time.time())}"
            await r.set("dnd:startup_test", test_val)
            readback = await r.get("dnd:startup_test")
            print(f"[main] Тестовая запись в Redis: {readback}", flush=True)

            await load_state()
            print("[main] load_state() завершён", flush=True)

        except Exception as e:
            print(f"[main] ОШИБКА Redis: {e}", flush=True)
            traceback.print_exc()
            r = None
    else:
        print("[main] REDIS_URL не задан!", flush=True)

    asyncio.create_task(autosave_loop())
    print("[main] autosave_loop запущен", flush=True)

    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"[main] Server started on port {port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
