import asyncio
import websockets
import json
import time
import os
import uuid
import traceback
import upstash_redis.asyncio as redis_async

UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")
r = None

clock_state = {
    "isRunning": False,
    "baseTime": 1704067200,
    "lastUpdateTimestamp": time.time()
}

CATEGORIES = ("clothing", "weapon", "consumables", "artifact", "other")
POLITICS_KEYS = ("comintern", "moralintern", "neutral", "redFeathers", "utopia")
CHARACTERISTIC_KEYS = ("physiology", "psyche", "intellect", "motorica")

players_data = {}
clients = set()

base_inventories = {"1": [], "2": []}
base_royals_dict = {"1": 0, "2": 0}
item_templates = []
pending_trades = []

save_lock = asyncio.Lock()

save_lock = asyncio.Lock()
last_saved_data = None # <-- Добавляем эту строку


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

def get_default_skills():
    return {
        "totalLevel": 0,
        "characteristics": {k: 1 for k in CHARACTERISTIC_KEYS},
        "abilities": {}
    }

def get_default_player():
    return {
        "anxiety": [], "inventory": [], "body": get_default_body(),
        "morale": 10, "anxietyLevel": 0.0, "royals": 0,
        "politics": get_default_politics(), "skills": get_default_skills()
    }


def get_default_templates():
    return [
        {"name": "Бинты", "desc": "Уменьшает степень кровотечения на 1, снимает половину накопившегося. Постепенно (каждый час или раз в 5 ходов, 3 ХП) восстанавливает ХП части тела, на которую наложена (максимум 5), после чего нужно поменять, иначе каждый час кидается д20 на инфекцию (с 16 инфекция).", "category": "consumables", "count": 1},
        {"name": "Шина", "desc": "Убирает дебаффы от перелома или вывиха, конечность восстановится через 2/3/5 дней. Вывих всегда нужно вправлять. Заменяет дебафф на -1 к боевому духу каждый час.", "category": "consumables", "count": 1},
        {"name": "Экспериментальный антибиотик", "desc": "При приёме уменьшает степень сепсиса и любой бактериальной инфекции на 1.", "category": "consumables", "count": 1},
        {"name": "Хирургическая пила", "desc": "За час (5 ходов в битве) позволяет ампутировать конечность, предотвратив кровотечение. Проверка д20 на психику или интеллект, удача 12.", "category": "consumables", "count": 1},
        {"name": "Набор для шитья", "desc": "За несколько минут (1 ход) позволяет убрать любые ранения полностью. Удача 12 на психику.", "category": "consumables", "count": 1},
        {"name": "Лечебная мазь", "desc": "Постепенно восстанавливает ХП части тела (каждый час или раз в 5 ходов в размере 5 ХП), уменьшает степень ожога (снимает 5 ожога).", "category": "consumables", "count": 1},
        {"name": "Зажим", "desc": "Временно (д4 часа или хода) останавливает кровотечение. Уменьшает степень ранения. Если оставить дольше, каждый ход/час требуется д20 на инфекцию (удача 16).", "category": "consumables", "count": 1},
        {"name": "Обезболивающее (наркотик)", "desc": "Содержит героин. При частом употреблении (более 4 раз в день) вызывает привыкание. Убирает все дебаффы на половину от Травм. Действует 2 часа (весь бой).", "category": "consumables", "count": 1},
        {"name": "Ненаркотические обезболивающие", "desc": "Не вызывают привыкания, уменьшают дебаффы от травм лишь на четверть (округляя вниз). Действуют 5 часов.", "category": "consumables", "count": 1},
        {"name": "Психотропик «Психнет»", "desc": "Временно (д4+4 часов) блокирует проявления психической болезни, -2 к результату на все проверки, -3 на социальные проверки и попадание. После действия расстройство усиливается в 2 раза на 2 часа (кроме амнезии).", "category": "consumables", "count": 1},
        {"name": "Психотропик «Психнет+» (наркотик)", "desc": "Временно (д8+4 часов) блокирует проявления психической болезни, +2 к результату на все проверки, -3 на социальные проверки и попадание. Нельзя принимать больше 4 раз в день. При употреблении после 4 приёмов может убрать Психическое заболевание навсегда (д20, удача 20).", "category": "consumables", "count": 1},
        {"name": "Препарат «Ренинганг»", "desc": "Убирает Передозировку, однако уменьшает Боевой дух до 3.", "category": "consumables", "count": 1},
        {"name": "Пиво «БиерБрудер» (наркотик)", "desc": "Слабый алкогольный напиток. +1 к Боевому духу, -1 к попаданию, стойкости и самообладанию. 2 часа. Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Вино «Виолет» (наркотик)", "desc": "Средний алкогольный напиток. +2 к Боевому духу, -2 к попаданию, стойкости и самообладанию. 2 часа. Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Шнампс «Летзтер» (наркотик)", "desc": "Сильный алкогольный напиток. +5 к Боевому духу, -3 к попаданию, стойкости и самообладанию. 2 часа. Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Сигарета «Рауч» (наркотик)", "desc": "Слабый никотиновый продукт. -0.5 к Тревоге, +1 к следующему броску на концентрацию или попадание, -1 к следующему социальному броску. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Сигара «Дикер» (наркотик)", "desc": "Средний никотиновый продукт. -1 к Тревоге, +2 к следующему броску на концентрацию или попадание, -2 к следующему социальному броску. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Никотиновая инъекция (наркотик)", "desc": "Сильный никотиновый продукт. -4 к Тревоге, +4 к следующему броску на концентрацию или попадание, -4 к следующему социальному броску. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Дрог (наркотик)", "desc": "+5 к попаданию, концентрации, физиологии, -3 к социальным. Эйфория. 2 часа, после чего «отходняк»: -2 ко всем проверкам.", "category": "consumables", "count": 1},
        {"name": "Героин (наркотик)", "desc": "Нивелирует дебаффы от всех Травм, -5 к самоконтролю и социальному взаимодействию. Эйфория, сонливость. 2 часа, после чего -2 ко всем характеристикам.", "category": "consumables", "count": 1},
        {"name": "Гетран", "desc": "Кофейный напиток. +2 к моторике. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Препарат «Гехирм»", "desc": "Синие таблетки. +2 к психике. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Куриная ножка в соусе Стрипп", "desc": "+2 к физиологии. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Тёмный шоколад «Шок»", "desc": "+2 к интеллекту. 2 часа.", "category": "consumables", "count": 1},
        {"name": "Морское пальто", "desc": "Туловище. +1 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"physiology": 1, "def": {"torso": 1}}},
        {"name": "Пиджак «Диско»", "desc": "Туловище. +1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"intellect": 1, "def": {"torso": 1}}},
        {"name": "Спортивная куртка", "desc": "Туловище. +1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"motorica": 1, "def": {"torso": 1}}},
        {"name": "Красный мундир Эрстании", "desc": "Туловище. +2 к физиологии, -1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"physiology": 2, "intellect": -1, "def": {"torso": 2}}},
        {"name": "Чёрный плащ", "desc": "Туловище. -3 к попаданию противника, +1 к интеллекту и психологии, -2 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"intellect": 1, "psyche": 1, "physiology": -2, "def": {"torso": 1}}},
        {"name": "Широкополая шляпа с белыми цветами", "desc": "Голова. +1 к психике.", "category": "clothing", "count": 1, "bonuses": {"psyche": 1, "def": {"head": 0}}},
        {"name": "Обтягивающая спортивная шапочка", "desc": "Голова. +1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"motorica": 1, "def": {"head": 1}}},
        {"name": "Резинка для головы", "desc": "Голова. +1 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"physiology": 1, "def": {"head": 0}}},
        {"name": "Толстые круглые очки", "desc": "Голова. +1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"intellect": 1, "def": {"head": 0}}},
        {"name": "Оранжевая шапка", "desc": "Голова. +1 к физиологии и моторике, -1 к психике.", "category": "clothing", "count": 1, "bonuses": {"physiology": 1, "motorica": 1, "psyche": -1, "def": {"head": 1}}},
        {"name": "Старый кивер Эрстанской армии", "desc": "Голова. +2 к физиологии, -1 к психике.", "category": "clothing", "count": 1, "bonuses": {"physiology": 2, "psyche": -1, "def": {"head": 2}}},
        {"name": "Бриджи", "desc": "Ноги. +1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"motorica": 1, "def": {"leftLeg": 1, "rightLeg": 1}}},
        {"name": "Полицейские брюки", "desc": "Ноги. +1 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"physiology": 1, "def": {"leftLeg": 1, "rightLeg": 1}}},
        {"name": "Песчаные брюки с молнией", "desc": "Ноги. +1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"intellect": 1, "def": {"leftLeg": 1, "rightLeg": 1}}},
        {"name": "Серо-коричневые штаны с ремнём", "desc": "Ноги. +1 к психике.", "category": "clothing", "count": 1, "bonuses": {"psyche": 1, "def": {"leftLeg": 1, "rightLeg": 1}}},
        {"name": "Шерстяные штаны до колена", "desc": "Ноги. +2 к моторике, -1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"motorica": 2, "intellect": -1, "def": {"leftLeg": 1, "rightLeg": 1}}},
        {"name": "Штаны легионера Эрстании", "desc": "Ноги. +2 к физиологии, -1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"physiology": 2, "motorica": -1, "def": {"leftLeg": 2, "rightLeg": 2}}},
        {"name": "Отвратительные зелёные туфли", "desc": "Ступни. +1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"motorica": 1}},
        {"name": "Шуе", "desc": "Ступни. +1 к интеллекту.", "category": "clothing", "count": 1, "bonuses": {"intellect": 1}},
        {"name": "Берды", "desc": "Ступни. +1 к психике.", "category": "clothing", "count": 1, "bonuses": {"psyche": 1}},
        {"name": "Длинные кожаные военные ботинки", "desc": "Ступни. +1 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"physiology": 1}},
        {"name": "Чёрные туфли с упряжкой", "desc": "Ступни. +2 к интеллекту, -1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"intellect": 2, "motorica": -1}},
        {"name": "Красные башмаки гангстера", "desc": "Ступни. +2 к физиологии, -1 к психике.", "category": "clothing", "count": 1, "bonuses": {"physiology": 2, "psyche": -1}},
        {"name": "Защитный костюм Граухайт М1", "desc": "Покрывает всё тело. +2 к интеллекту, -1 к моторике.", "category": "clothing", "count": 1, "bonuses": {"intellect": 2, "motorica": -1, "def": {"head": 1, "torso": 2, "leftArm": 2, "rightArm": 2, "leftLeg": 2, "rightLeg": 2}}},
        {"name": "Открытое платье", "desc": "+2 к психике, +1 к интеллекту, -3 к моторике.", "category": "clothing", "count": 1, "bonuses": {"psyche": 2, "intellect": 1, "motorica": -3}},
        {"name": "Медицинский халат", "desc": "+3 к психике, +1 к интеллекту, -3 к моторике, -1 к физиологии.", "category": "clothing", "count": 1, "bonuses": {"psyche": 3, "intellect": 1, "motorica": -3, "physiology": -1}},
        {"name": "ПОЗБ (пистолет)", "desc": "Попадание 8, дальность 3/6, урон д8.", "category": "weapon", "count": 1},
        {"name": "Барабанный пистолет", "desc": "Попадание 10, дальность 4/6, урон д10.", "category": "weapon", "count": 1},
        {"name": "Винтовка Бернара БУ", "desc": "Попадание 10, дальность 4/8, урон д12.", "category": "weapon", "count": 1},
        {"name": "Самопальный пистоль", "desc": "Попадание 13, дальность 2/4, урон д6.", "category": "weapon", "count": 1},
        {"name": "Винтовка Стард М1", "desc": "Попадание 10, дальность 2/4, урон д4.", "category": "weapon", "count": 1},
        {"name": "Кремневая винтовка Дугина", "desc": "Попадание 12, дальность 5/10, урон д10.", "category": "weapon", "count": 1},
        {"name": "Винтовка Бернара Б", "desc": "Попадание 10, дальность 6/12, урон д14.", "category": "weapon", "count": 1},
        {"name": "Фастплаттер Модель 10", "desc": "Попадание 13, дальность 2/4, урон д12, конус.", "category": "weapon", "count": 1},
        {"name": "Фастплаттер х89", "desc": "Попадание 14, дальность 1/2, урон 5д4, конус.", "category": "weapon", "count": 1},
        {"name": "Гевехр", "desc": "Попадание 15, дальность 5/10, урон д6.", "category": "weapon", "count": 1},
        {"name": "Самопальная винтовка", "desc": "Попадание 15, дальность 4/8.", "category": "weapon", "count": 1},
        {"name": "Армейский нож", "desc": "Попадание 8, урон д6.", "category": "weapon", "count": 1},
        {"name": "Лом (оружие)", "desc": "Попадание 5, урон д6.", "category": "weapon", "count": 1},
        {"name": "Полицейская дубинка", "desc": "Попадание 8, урон 0.", "category": "weapon", "count": 1},
        {"name": "Лом (инструмент)", "desc": "+3 к взлому.", "category": "other", "count": 1},
        {"name": "Кусачки", "desc": "+3 к технике.", "category": "other", "count": 1},
        {"name": "Фонарик", "desc": "Видеть в темноте.", "category": "other", "count": 1},
        {"name": "Бинокль", "desc": "+2 эффективной, +4 макс дальности.", "category": "other", "count": 1},
        {"name": "Пила", "desc": "+3 к дереву.", "category": "other", "count": 1},
        {"name": "Электрогазосварка", "desc": "+3 к металлу.", "category": "other", "count": 1},
    ]


def ensure_player_shape(name):
    if name not in players_data:
        players_data[name] = get_default_player()
        return
    p = players_data[name]
    if "anxiety" not in p or not isinstance(p["anxiety"], list): p["anxiety"] = []
    if "inventory" not in p or not isinstance(p["inventory"], list): p["inventory"] = []
    if "body" not in p or not isinstance(p["body"], dict): p["body"] = get_default_body()
    if "morale" not in p: p["morale"] = 10
    if "anxietyLevel" not in p: p["anxietyLevel"] = 0.0
    if "royals" not in p: p["royals"] = 0
    if "politics" not in p or not isinstance(p["politics"], dict):
        p["politics"] = get_default_politics()
    else:
        for k in POLITICS_KEYS:
            if k not in p["politics"]: p["politics"][k] = 0
    if "skills" not in p or not isinstance(p["skills"], dict):
        p["skills"] = get_default_skills()
    else:
        sk = p["skills"]
        if "totalLevel" not in sk: sk["totalLevel"] = 0
        if "characteristics" not in sk or not isinstance(sk["characteristics"], dict):
            sk["characteristics"] = {k: 1 for k in CHARACTERISTIC_KEYS}
        else:
            for k in CHARACTERISTIC_KEYS:
                if k not in sk["characteristics"]:
                    sk["characteristics"][k] = 1
        if "abilities" not in sk or not isinstance(sk["abilities"], dict):
            sk["abilities"] = {}


def parse_ability_id(ability_id):
    if not isinstance(ability_id, str):
        return None
    parts = ability_id.split(".")
    if len(parts) != 3:
        return None
    char = parts[0]
    if char not in CHARACTERISTIC_KEYS:
        return None
    try:
        tier = int(parts[1]); num = int(parts[2])
    except ValueError:
        return None
    if tier < 1 or tier > 5 or num < 1 or num > 3:
        return None
    return {"char": char, "tier": tier, "num": num}


def skills_spent(skills):
    """Очки тратятся только на способности, не на характеристики."""
    spent = 0
    for aid, lvl in skills.get("abilities", {}).items():
        try:
            spent += max(0, int(lvl))
        except (TypeError, ValueError):
            pass
    return spent


def skills_available(skills):
    total = int(skills.get("totalLevel", 0) or 0)
    return max(0, total - skills_spent(skills))


def has_investment_in_tier(skills, char, tier):
    for aid, lvl in skills.get("abilities", {}).items():
        if int(lvl or 0) <= 0:
            continue
        p = parse_ability_id(aid)
        if p and p["char"] == char and p["tier"] == tier:
            return True
    return False


# ================================================================
#                    СОХРАНЕНИЕ / ЗАГРУЗКА
# ================================================================
async def save_state():
    global last_saved_data
    if not r: return
    async with save_lock:
        try:
            # Собираем все данные в один словарь
            payload = {
                "dnd:clock_state": json.dumps(clock_state),
                "dnd:players_data": json.dumps(players_data, ensure_ascii=False),
                "dnd:base_inventories": json.dumps(base_inventories, ensure_ascii=False),
                "dnd:base_royals_dict": json.dumps(base_royals_dict),
                "dnd:item_templates": json.dumps(item_templates, ensure_ascii=False),
                "dnd:pending_trades": json.dumps(pending_trades, ensure_ascii=False)
            }
            
            # Если данные не изменились с прошлого раза — ничего не отправляем
            current_data_str = str(payload)
            if current_data_str == last_saved_data:
                return
            
            # Отправляем всё одной командой MSET вместо 6 разных SET
            await r.mset(payload)
            last_saved_data = current_data_str
            
        except Exception as e:
            print(f"[save_state] Ошибка: {e}", flush=True)


async def load_state():
    global base_royals
    if not r: return
    try:
        d = await r.get("dnd:clock_state")
        if d: clock_state.update(json.loads(d))

        d = await r.get("dnd:players_data")
        if d:
            players_data.clear()
            players_data.update(json.loads(d))
            for n in list(players_data.keys()):
                ensure_player_shape(n)

        d = await r.get("dnd:base_inventories")
        if d:
            base_inventories.clear()
            base_inventories.update(json.loads(d))

        d = await r.get("dnd:base_royals_dict")
        if d: 
            base_royals_dict.clear()
            base_royals_dict.update(json.loads(d))

        d = await r.get("dnd:item_templates")
        loaded_templates = []
        if d:
            try: loaded_templates = json.loads(d)
            except Exception: loaded_templates = []
        item_templates.clear()
        if loaded_templates:
            item_templates.extend(loaded_templates)
        else:
            flag = await r.get("dnd:templates_initialized")
            if not flag:
                item_templates.extend(get_default_templates())
                await r.set("dnd:templates_initialized", "1")

        changed = False
        for tpl in item_templates:
            if not tpl.get("id"):
                tpl["id"] = new_id(); changed = True
        if changed:
            await r.set("dnd:item_templates", json.dumps(item_templates, ensure_ascii=False))

        d = await r.get("dnd:pending_trades")
        if d:
            pending_trades.clear()
            pending_trades.extend(json.loads(d))

        print(f"[load_state] Загружено: {len(players_data)} игроков", flush=True)
    except Exception as e:
        print(f"[load_state] Ошибка: {e}", flush=True)
        traceback.print_exc()


async def autosave_loop():
    while True:
        await asyncio.sleep(30)
        await save_state()


def new_id(): return uuid.uuid4().hex[:12]

def safe_int(v, default=0):
    try: return int(v)
    except (TypeError, ValueError): return default

def add_item_to_bucket(bucket, item, count=None):
    if count is None: count = safe_int(item.get("count", 1), 1)
    count = max(1, count)
    name, desc, cat = item.get("name", ""), item.get("desc", ""), safe_category(item.get("category", "other"))
    for existing in bucket:
        if existing.get("name") == name and (existing.get("desc") or "") == desc and safe_category(existing.get("category", "other")) == cat:
            existing["count"] = int(existing.get("count", 1) or 1) + count
            return existing
    new_item = {"id": new_id(), "name": name, "desc": desc, "category": cat, "count": count}
    bucket.append(new_item)
    return new_item

def find_item(lst, item_id):
    if not isinstance(lst, list): return None
    if item_id is None: return None
    for i in lst:
        if str(i.get("id")) == str(item_id): return i
    return None

def take_from_stack(lst, item_id, count):
    item = find_item(lst, item_id)
    if not item: return None
    avail = int(item.get("count", 1) or 1)
    if avail <= 0: return None
    count = safe_int(count, 0)
    if count <= 0: return None
    take = min(count, avail)
    item["count"] = avail - take
    snapshot = {"name": item.get("name", "") or "", "desc": item.get("desc", "") or "", "category": safe_category(item.get("category", "other"))}
    if item["count"] <= 0:
        try: lst.remove(item)
        except ValueError: pass
    return snapshot, take


async def broadcast_players():
    if clients:
        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

async def broadcast_social():
    if clients:
        websockets.broadcast(clients, json.dumps({
            "type": "sync_social",
            "baseInventories": base_inventories,
            "baseRoyalsDict": base_royals_dict,
            "itemTemplates": item_templates,
            "pendingTrades": pending_trades
        }))

async def broadcast_event(kind, data):
    if clients:
        websockets.broadcast(clients, json.dumps({"type": "event", "kind": kind, "data": data}))


async def handler(websocket):
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "sync", "state": clock_state}))
        await websocket.send(json.dumps({"type": "sync_players", "playersData": players_data}))
        await websocket.send(json.dumps({
            "type": "sync_social",
            "baseInventories": base_inventories, 
            "baseRoyalsDict": base_royals_dict,
            "itemTemplates": item_templates, 
            "pendingTrades": pending_trades
        }))

        async for message in websocket:
            try: data = json.loads(message)
            except json.JSONDecodeError: continue

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
                target, part_id, new_hp = data.get("target"), data.get("part"), data.get("hp")
                new_def, new_max = data.get("def", 0), data.get("maxHp")
                if target:
                    ensure_player_shape(target)
                    p = players_data[target]
                    if part_id == "morale":
                        try: p["morale"] = max(0.0, float(new_hp))
                        except (TypeError, ValueError): pass
                        if new_max is not None:
                            try: p["maxMorale"] = max(0, int(new_max))
                            except (TypeError, ValueError): pass
                    elif part_id == "anxietyLevel":
                        try: p["anxietyLevel"] = max(0.0, float(new_hp))
                        except (TypeError, ValueError): pass
                        if new_max is not None:
                            try: p["maxAnxiety"] = max(0, int(new_max))
                            except (TypeError, ValueError): pass
                    elif part_id in p["body"]:
                        try: hp, df = int(new_hp), int(new_def)
                        except (TypeError, ValueError): hp, df = None, None
                        if hp is not None:
                            max_hp = p["body"][part_id].get("maxHp", hp)
                            if new_max is not None:
                                try:
                                    max_hp = max(0, int(new_max))
                                    p["body"][part_id]["maxHp"] = max_hp
                                except (TypeError, ValueError): pass
                            p["body"][part_id]["hp"] = max(0, min(max_hp, hp))
                            p["body"][part_id]["def"] = max(0, df)
                    await broadcast_players()

            elif msg_type == "update_royals":
                target, amount = data.get("target"), data.get("amount")
                if target:
                    ensure_player_shape(target)
                    players_data[target]["royals"] = max(0, safe_int(amount, 0))
                    await broadcast_players()

            elif msg_type == "base_royals_set":
                base_royals = max(0, safe_int(data.get("amount"), 0))
                await broadcast_social()

            elif msg_type == "transfer_royals":
                src, dst, amount = data.get("from"), data.get("to"), max(0, safe_int(data.get("amount"), 0))
                if src and dst and amount > 0:
                    ensure_player_shape(src); ensure_player_shape(dst)
                    src_r = int(players_data[src].get("royals", 0) or 0)
                    if src_r >= amount:
                        players_data[src]["royals"] = src_r - amount
                        players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) + amount
                        await broadcast_players()
                        await broadcast_event("royals_transfer", {"from": src, "to": dst, "amount": amount})

            elif msg_type == "update_politics":
                target, key = data.get("target"), data.get("key")
                if target and key in POLITICS_KEYS:
                    ensure_player_shape(target)
                    cur = int(players_data[target]["politics"].get(key, 0) or 0)
                    if "delta" in data: new_val = cur + safe_int(data["delta"], 0)
                    elif "value" in data: new_val = safe_int(data["value"], cur)
                    else: new_val = cur
                    players_data[target]["politics"][key] = new_val
                    await broadcast_players()

            # ================================================
            # РАЗВИТИЕ
            # ================================================
            elif msg_type == "skills_set_level":
                target = data.get("target")
                level = max(0, safe_int(data.get("level"), 0))
                if target:
                    ensure_player_shape(target)
                    players_data[target]["skills"]["totalLevel"] = level
                    await broadcast_players()
                    await save_state()

            elif msg_type == "skills_reset":
                target = data.get("target")
                if target:
                    ensure_player_shape(target)
                    players_data[target]["skills"] = get_default_skills()
                    await broadcast_players()
                    await save_state()


            elif msg_type == "skills_invest_ability":
                player = data.get("player")
                aid = data.get("abilityId")
                parsed = parse_ability_id(aid)
                if player and parsed:
                    ensure_player_shape(player)
                    sk = players_data[player]["skills"]
                    char = parsed["char"]
                    tier = parsed["tier"]
                    char_level = int(sk["characteristics"].get(char, 1) or 1)

                    # Разрешение: либо ручной уровень достаточен,
                    # либо есть вложение в предыдущий ярус
                    tier_unlocked = False
                    if tier == 1:
                        tier_unlocked = True
                    elif char_level >= tier:
                        tier_unlocked = True
                    elif has_investment_in_tier(sk, char, tier - 1):
                        tier_unlocked = True

                    if tier_unlocked and skills_available(sk) > 0:
                        cur = int(sk.get("abilities", {}).get(aid, 0) or 0)
                        if cur < 3:
                            sk.setdefault("abilities", {})[aid] = cur + 1
                            # Автоматическое повышение характеристики за каждое вложение
                            sk["characteristics"][char] = int(sk["characteristics"].get(char, 1)) + 1
                            await broadcast_players()
                            await save_state()

            elif msg_type == "skills_remove_ability":
                player = data.get("player")
                aid = data.get("abilityId")
                if player and aid:
                    ensure_player_shape(player)
                    sk = players_data[player]["skills"]
                    cur = int(sk.get("abilities", {}).get(aid, 0) or 0)
                    if cur > 0:
                        if cur - 1 <= 0:
                            sk["abilities"].pop(aid, None)
                        else:
                            sk["abilities"][aid] = cur - 1
                        # Автоматическое понижение характеристики при отмене вложения
                        sk["characteristics"][char] = max(1, int(sk["characteristics"].get(char, 2)) - 1)
                        await broadcast_players()
                        await save_state()

            # ================================================
            # ПРЕДМЕТЫ
            # ================================================
            elif msg_type == "update_player_data":
                target, data_type, item = data.get("target"), data.get("dataType"), data.get("item")
                if target and data_type and item:
                    ensure_player_shape(target)
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        if data_type == "inventory": add_item_to_bucket(bucket, item)
                        else: bucket.append(item)
                        await broadcast_players()

            elif msg_type == "delete_item":
                target, data_type, item_id = data.get("target"), data.get("dataType"), data.get("itemId")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        players_data[target][data_type] = [i for i in bucket if str(i.get("id")) != str(item_id)]
                        await broadcast_players()

            elif msg_type == "edit_item":
                target, data_type, new_item = data.get("target"), data.get("dataType"), data.get("item")
                if target and data_type and target in players_data:
                    bucket = players_data[target].get(data_type)
                    if isinstance(bucket, list):
                        for i in range(len(bucket)):
                            if str(bucket[i].get("id")) == str(new_item.get("id")):
                                merged = dict(new_item); merged["id"] = bucket[i].get("id")
                                if data_type == "inventory":
                                    merged["category"] = safe_category(merged.get("category", "other"))
                                    merged["count"] = max(1, safe_int(merged.get("count", 1), 1))
                                    merged["bonuses"] = new_item.get("bonuses", {})
                                bucket[i] = merged; break
                        await broadcast_players()

            elif msg_type == "transfer_item":
                src, dst, data_type = data.get("from"), data.get("to"), data.get("dataType", "inventory")
                item_id, count = data.get("itemId"), data.get("count", 1)
                if src and dst and item_id:
                    ensure_player_shape(src); ensure_player_shape(dst)
                    bucket = players_data[src].get(data_type, [])
                    res = take_from_stack(bucket, item_id, count)
                    if res:
                        snapshot, taken = res
                        if data_type == "inventory": add_item_to_bucket(players_data[dst].setdefault(data_type, []), snapshot, taken)
                        else: players_data[dst].setdefault(data_type, []).append(snapshot)
                        await broadcast_players()
                        await broadcast_event("transfer", {"from": src, "to": dst, "item": snapshot, "count": taken, "dataType": data_type})

            elif msg_type == "base_add":
                item, gid = data.get("item"), str(data.get("groupId", "1"))
                if item:
                    add_item_to_bucket(base_inventories.setdefault(gid, []), item)
                    await broadcast_social()

            elif msg_type == "base_edit":
                new_item, gid = data.get("item"), str(data.get("groupId", "1"))
                if new_item:
                    existing = find_item(base_inventories.setdefault(gid, []), new_item.get("id"))
                    if existing:
                        existing["name"] = new_item.get("name", existing.get("name"))
                        existing["desc"] = new_item.get("desc", existing.get("desc") or "")
                        existing["category"] = safe_category(new_item.get("category", existing.get("category", "other")))
                        existing["count"] = max(1, safe_int(new_item.get("count", existing.get("count", 1)), 1))
                        existing["bonuses"] = new_item.get("bonuses", existing.get("bonuses", {}))
                        snapshot = {"name": tpl.get("name", ""), "desc": tpl.get("desc", "") or "", "category": safe_category(tpl.get("category", "other")), "bonuses": tpl.get("bonuses", {})}
                        await broadcast_social()

            elif msg_type == "base_delete":
                item_id, gid = data.get("itemId"), str(data.get("groupId", "1"))
                if item_id:
                    bucket = base_inventories.setdefault(gid, [])
                    for idx, it in enumerate(bucket):
                        if str(it.get("id")) == str(item_id):
                            bucket.pop(idx); break
                    await broadcast_social()

            elif msg_type == "base_put":
                src, item_id, count = data.get("from"), data.get("itemId"), data.get("count", 1)
                gid = str(data.get("groupId", "1"))
                if src and item_id:
                    ensure_player_shape(src)
                    bucket = players_data[src].get("inventory", [])
                    res = take_from_stack(bucket, item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(base_inventories.setdefault(gid, []), snapshot, taken)
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_put", {"from": src, "item": snapshot, "count": taken})

            elif msg_type == "base_take":
                player, base_item_id, count = data.get("player"), data.get("baseItemId"), data.get("count", 1)
                gid = str(data.get("groupId", "1"))
                if player and base_item_id:
                    ensure_player_shape(player)
                    res = take_from_stack(base_inventories.setdefault(gid, []), base_item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(players_data[player].setdefault("inventory", []), snapshot, taken)
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_take", {"player": player, "item": snapshot, "count": taken})

            elif msg_type == "base_royals_put":
                player, amount = data.get("player"), max(0, safe_int(data.get("amount"), 0))
                gid = str(data.get("groupId", "1"))
                if player and amount > 0:
                    ensure_player_shape(player)
                    have = int(players_data[player].get("royals", 0) or 0)
                    if have >= amount:
                        players_data[player]["royals"] = have - amount
                        base_royals_dict[gid] = base_royals_dict.get(gid, 0) + amount
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_royals_put", {"player": player, "amount": amount})

            elif msg_type == "base_royals_take":
                player, amount = data.get("player"), max(0, safe_int(data.get("amount"), 0))
                gid = str(data.get("groupId", "1"))
                cur_base_royals = base_royals_dict.get(gid, 0)
                if player and amount > 0 and cur_base_royals >= amount:
                    ensure_player_shape(player)
                    base_royals_dict[gid] = cur_base_royals - amount
                    players_data[player]["royals"] = int(players_data[player].get("royals", 0) or 0) + amount
                    await broadcast_players(); await broadcast_social()
                    await broadcast_event("base_royals_take", {"player": player, "amount": amount})

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
                            item_templates.pop(idx); break
                    await broadcast_social()

            elif msg_type == "template_give":
                template_id, target, count = data.get("templateId"), data.get("target"), data.get("count", None)
                if template_id and target:
                    tpl = find_item(item_templates, template_id)
                    if tpl:
                        ensure_player_shape(target)
                        snapshot = {"name": tpl.get("name", ""), "desc": tpl.get("desc", "") or "", "category": safe_category(tpl.get("category", "other"))}
                        give_count = safe_int(count, safe_int(tpl.get("count", 1), 1)) if count is not None else safe_int(tpl.get("count", 1), 1)
                        give_count = max(1, give_count)
                        add_item_to_bucket(players_data[target].setdefault("inventory", []), snapshot, give_count)
                        await broadcast_players()
                        await broadcast_event("template_give", {"target": target, "item": snapshot, "count": give_count})

            elif msg_type == "trade_propose":
                src, dst = data.get("from"), data.get("to")
                from_type, to_type = data.get("fromType", "item"), data.get("toType", "item")
                if not src or not dst: continue
                ensure_player_shape(src); ensure_player_shape(dst)
                from_item_id, from_count, from_royals = data.get("fromItemId"), max(1, safe_int(data.get("fromCount", 1), 1)), max(0, safe_int(data.get("fromRoyals", 0), 0))
                to_item_id, to_count, to_royals = data.get("toItemId"), max(1, safe_int(data.get("toCount", 1), 1)), max(0, safe_int(data.get("toRoyals", 0), 0))
                f_ok, f_snap = False, None
                if from_type == "royals":
                    if int(players_data[src].get("royals", 0) or 0) >= from_royals and from_royals > 0:
                        f_ok = True; f_snap = {"type": "royals", "amount": from_royals}
                else:
                    f_item = find_item(players_data[src].get("inventory", []), from_item_id)
                    if f_item:
                        f_avail = int(f_item.get("count", 1) or 1)
                        from_count = min(from_count, f_avail)
                        f_ok = True
                        f_snap = {"type": "item", "itemId": from_item_id, "count": from_count,
                                  "item": {"name": f_item.get("name", ""), "desc": f_item.get("desc", "") or "", "category": safe_category(f_item.get("category", "other"))}}
                t_ok, t_snap = False, None
                if to_type == "royals":
                    if int(players_data[dst].get("royals", 0) or 0) >= to_royals and to_royals > 0:
                        t_ok = True; t_snap = {"type": "royals", "amount": to_royals}
                else:
                    t_item = find_item(players_data[dst].get("inventory", []), to_item_id)
                    if t_item:
                        t_avail = int(t_item.get("count", 1) or 1)
                        to_count = min(to_count, t_avail)
                        t_ok = True
                        t_snap = {"type": "item", "itemId": to_item_id, "count": to_count,
                                  "item": {"name": t_item.get("name", ""), "desc": t_item.get("desc", "") or "", "category": safe_category(t_item.get("category", "other"))}}
                if f_ok and t_ok:
                    trade = {
                        "id": new_id(), "from": src, "to": dst,
                        "fromType": from_type, "fromRoyals": from_royals if from_type == "royals" else 0,
                        "fromItemId": from_item_id if from_type == "item" else None, "fromCount": from_count if from_type == "item" else 0,
                        "fromItem": (f_snap.get("item") if f_snap and from_type == "item" else None),
                        "toType": to_type, "toRoyals": to_royals if to_type == "royals" else 0,
                        "toItemId": to_item_id if to_type == "item" else None, "toCount": to_count if to_type == "item" else 0,
                        "toItem": (t_snap.get("item") if t_snap and to_type == "item" else None),
                        "fromConfirmed": True, "toConfirmed": False, "createdAt": time.time()
                    }
                    pending_trades.append(trade)
                    await broadcast_social()
                    await broadcast_event("trade_proposed", trade)
                else:
                    await broadcast_event("trade_failed", {"reason": "missing_item"})

            elif msg_type == "trade_confirm":
                trade_id, player = data.get("tradeId"), data.get("player")
                trade = next((t for t in pending_trades if t["id"] == trade_id), None)
                if trade and player in (trade["from"], trade["to"]):
                    if player == trade["from"]: trade["fromConfirmed"] = True
                    else: trade["toConfirmed"] = True
                    if trade["fromConfirmed"] and trade["toConfirmed"]:
                        src, dst = trade["from"], trade["to"]
                        ok, f_side, t_side = True, None, None
                        if trade["fromType"] == "royals":
                            have = int(players_data[src].get("royals", 0) or 0)
                            if have < trade["fromRoyals"]: ok = False
                            else: f_side = {"type": "royals", "amount": trade["fromRoyals"]}
                        else:
                            f_item = take_from_stack(players_data[src].get("inventory", []), trade["fromItemId"], trade["fromCount"])
                            if not f_item: ok = False
                            else: f_snap, f_taken = f_item; f_side = {"type": "item", "item": f_snap, "count": f_taken}
                        if ok:
                            if trade["toType"] == "royals":
                                have = int(players_data[dst].get("royals", 0) or 0)
                                if have < trade["toRoyals"]: ok = False
                                else: t_side = {"type": "royals", "amount": trade["toRoyals"]}
                            else:
                                t_item = take_from_stack(players_data[dst].get("inventory", []), trade["toItemId"], trade["toCount"])
                                if not t_item: ok = False
                                else: t_snap, t_taken = t_item; t_side = {"type": "item", "item": t_snap, "count": t_taken}
                        if not ok:
                            if f_side and f_side["type"] == "item":
                                add_item_to_bucket(players_data[src].setdefault("inventory", []), f_side["item"], f_side["count"])
                            pending_trades.remove(trade)
                            await broadcast_players(); await broadcast_social()
                            await broadcast_event("trade_failed", {"tradeId": trade_id})
                        else:
                            if f_side["type"] == "royals":
                                players_data[src]["royals"] = int(players_data[src].get("royals", 0) or 0) - f_side["amount"]
                                players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) + f_side["amount"]
                            else:
                                add_item_to_bucket(players_data[dst].setdefault("inventory", []), f_side["item"], f_side["count"])
                            if t_side["type"] == "royals":
                                players_data[dst]["royals"] = int(players_data[dst].get("royals", 0) or 0) - t_side["amount"]
                                players_data[src]["royals"] = int(players_data[src].get("royals", 0) or 0) + t_side["amount"]
                            else:
                                add_item_to_bucket(players_data[src].setdefault("inventory", []), t_side["item"], t_side["count"])
                            pending_trades.remove(trade)
                            await broadcast_players(); await broadcast_social()
                            await broadcast_event("trade_completed", {"from": src, "to": dst, "fromSide": f_side, "toSide": t_side})
                    else:
                        await broadcast_social()

            elif msg_type == "trade_cancel":
                trade_id, player = data.get("tradeId"), data.get("player")
                trade = next((t for t in pending_trades if t["id"] == trade_id), None)
                if trade and (not player or player in (trade["from"], trade["to"])):
                    pending_trades.remove(trade)
                    await broadcast_social()
                    await broadcast_event("trade_cancelled", {"tradeId": trade_id, "by": player})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        clients.discard(websocket)


async def main():
    global r
    port = int(os.environ.get("PORT", 8765))
    print(f"[main] STARTUP, PORT={port}", flush=True)

    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        try:
            r = redis_async.Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)
            await r.set("dnd:startup_test", str(int(time.time())))
            await load_state()
            print("[main] load_state() завершён", flush=True)
        except Exception as e:
            print(f"[main] ОШИБКА Redis: {e}", flush=True)
            traceback.print_exc()
            r = None

    asyncio.create_task(autosave_loop())
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
