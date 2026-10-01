import asyncio
import websockets
import json
import time
import os
import uuid
import traceback
import upstash_redis.asyncio as redis_async

# ================================================================
#                   REDIS (UPSTASH REST API)
# ================================================================
UPSTASH_REDIS_REST_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

r = None

# ================================================================
#                       СОСТОЯНИЕ
# ================================================================
clock_state = {
    "isRunning": False,
    "baseTime": 1704067200,   # 01 Эроберий 02 после овс, 00:00
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


def get_default_templates():
    """Дефолтные заготовки для хранилища мастера."""
    return [
        # ================== МЕДИЦИНСКИЕ ==================
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

        # ================== ИНЫЕ РАСХОДНИКИ ==================
        {"name": "Пиво «БиерБрудер» (наркотик)", "desc": "Слабый алкогольный напиток. Может вызвать зависимость при 15+ за день. +1 к Боевому духу, -1 к попаданию, стойкости и самообладанию. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Вино «Виолет» (наркотик)", "desc": "Средний алкогольный напиток. Может вызвать зависимость при 10+ за день. +2 к Боевому духу, -2 к попаданию, стойкости и самообладанию. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Шнампс «Летзтер» (наркотик)", "desc": "Сильный алкогольный напиток. Может вызвать зависимость при 5+ за день. +5 к Боевому духу, -3 к попаданию, стойкости и самообладанию. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Сигарета «Рауч» (наркотик)", "desc": "Слабый никотиновый продукт. Зависимость при 10+ за день. -0.5 к Тревоге, +1 к следующему броску на концентрацию или попадание, -1 к следующему социальному броску. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Сигара «Дикер» (наркотик)", "desc": "Средний никотиновый продукт. Зависимость при 5+ за день. -1 к Тревоге, +2 к следующему броску на концентрацию или попадание, -2 к следующему социальному броску. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Никотиновая инъекция (наркотик)", "desc": "Сильный никотиновый продукт. Зависимость при 2+ за день. -4 к Тревоге, +4 к следующему броску на концентрацию или попадание, -4 к следующему социальному броску. Действует 2 часа (4 хода). Эффекты суммируются.", "category": "consumables", "count": 1},
        {"name": "Дрог (наркотик)", "desc": "Самый распространённый наркотик в Эрде. +5 к попаданию, концентрации, физиологии, -3 к социальным. Эйфория (обнуляет тревогу, максимум боевого духа). Зависимость при 2+ в день. Действует 2 часа (4 хода), после чего «отходняк»: -2 ко всем проверкам. Эффекты не суммируются.", "category": "consumables", "count": 1},
        {"name": "Героин (наркотик)", "desc": "Сильный опиоид. Зависимость при 2+ в день. Нивелирует дебаффы от всех Травм, -5 к самоконтролю и социальному взаимодействию. Эйфория (обнуляет тревогу, максимум боевого духа), сонливость. Действует 2 часа (4 хода), после чего -2 ко всем характеристикам. Эффекты не суммируются.", "category": "consumables", "count": 1},
        {"name": "Гетран", "desc": "Кофейный напиток. +2 к моторике. Действует 2 часа (8 ходов). Эффект не суммируется.", "category": "consumables", "count": 1},
        {"name": "Препарат «Гехирм»", "desc": "Синие таблетки, не вызывают привыкания. +2 к психике. Действует 2 часа (8 ходов). Эффект не суммируется.", "category": "consumables", "count": 1},
        {"name": "Куриная ножка в соусе Стрипп", "desc": "+2 к физиологии. Действует 2 часа (8 ходов). Эффект не суммируется.", "category": "consumables", "count": 1},
        {"name": "Тёмный шоколад «Шок»", "desc": "+2 к интеллекту. Действует 2 часа (8 ходов). Эффект не суммируется.", "category": "consumables", "count": 1},

        # ================== ОДЕЖДА ==================
        {"name": "Морское пальто", "desc": "Туловище. +1 к физиологии.", "category": "clothing", "count": 1},
        {"name": "Пиджак «Диско»", "desc": "Туловище. +1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Спортивная куртка", "desc": "Туловище. +1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Красный мундир Эрстании", "desc": "Туловище. +2 к физиологии, -1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Чёрный плащ", "desc": "Туловище. -3 к попаданию противника, +1 к интеллекту и психологии, -2 к физиологии.", "category": "clothing", "count": 1},
        {"name": "Широкополая шляпа с белыми цветами", "desc": "Голова. +1 к психике.", "category": "clothing", "count": 1},
        {"name": "Обтягивающая спортивная шапочка", "desc": "Голова. +1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Резинка для головы", "desc": "Голова. +1 к физиологии.", "category": "clothing", "count": 1},
        {"name": "Толстые круглые очки", "desc": "Голова. +1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Оранжевая шапка", "desc": "Голова. +1 к физиологии и моторике, -1 к психике.", "category": "clothing", "count": 1},
        {"name": "Старый кивер Эрстанской армии", "desc": "Голова. +2 к физиологии, -1 к психике.", "category": "clothing", "count": 1},
        {"name": "Бриджи", "desc": "Ноги. +1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Полицейские брюки", "desc": "Ноги. +1 к физиологии.", "category": "clothing", "count": 1},
        {"name": "Песчаные брюки с молнией", "desc": "Ноги. +1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Серо-коричневые штаны с ремнём", "desc": "Ноги. +1 к психике.", "category": "clothing", "count": 1},
        {"name": "Шерстяные штаны до колена", "desc": "Ноги. +2 к моторике, -1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Штаны легионера Эрстании", "desc": "Ноги. +2 к физиологии, -1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Отвратительные зелёные туфли", "desc": "Ступни. +1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Шуе", "desc": "Ступни. +1 к интеллекту.", "category": "clothing", "count": 1},
        {"name": "Берды", "desc": "Ступни. +1 к психике.", "category": "clothing", "count": 1},
        {"name": "Длинные кожаные военные ботинки", "desc": "Ступни. +1 к физиологии.", "category": "clothing", "count": 1},
        {"name": "Чёрные туфли с упряжкой", "desc": "Ступни. +2 к интеллекту, -1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Красные башмаки гангстера", "desc": "Ступни. +2 к физиологии, -1 к психике.", "category": "clothing", "count": 1},
        {"name": "Защитный костюм Граухайт М1", "desc": "Покрывает всё тело. Нивелирует грязь и влияние Серости до 8 часов. При получении урона сразу разрушается на этой части тела, убирая бонусы. +2 к интеллекту, -1 к моторике.", "category": "clothing", "count": 1},
        {"name": "Открытое платье", "desc": "+2 к психике, +1 к интеллекту, -3 к моторике.", "category": "clothing", "count": 1},
        {"name": "Медицинский халат", "desc": "Уменьшает степень кровотечения и ожога на 1. +3 к психике, +1 к интеллекту, -3 к моторике, -1 к физиологии.", "category": "clothing", "count": 1},

        # ================== ОРУЖИЕ — ЛЁГКОЕ ==================
        {"name": "ПОЗБ (пистолет)", "desc": "Попадание 8, эффективная дальность 3, макс 6, урон д8. Один выстрел за ход, можно два пистолета. Каждый выстрел +0.1 Тревоги, при попадании +0.5.", "category": "weapon", "count": 1},
        {"name": "Барабанный пистолет", "desc": "Попадание 10, эффективная дальность 4, макс 6, урон д10. 5 доп. выстрелов за ход, каждый новый -2 к попаданию. Каждый выстрел +0.1 Тревоги, при попадании +0.5.", "category": "weapon", "count": 1},
        {"name": "Винтовка Бернара БУ", "desc": "Попадание 10, эффективная дальность 4, макс 8, урон д12. Два выстрела за ход. Каждый выстрел +0.2 Тревоги, при попадании +0.5.", "category": "weapon", "count": 1},
        {"name": "Самопальный пистоль", "desc": "Попадание 13, эффективная дальность 2, макс 4, урон д6. До 2 выстрелов за ход. Каждый выстрел +0.1 Тревоги, при попадании +0.2.", "category": "weapon", "count": 1},
        {"name": "Винтовка Стард М1", "desc": "Попадание 10, эффективная дальность 2, макс 4, урон д4. До 4 выстрелов за ход. При попадании в ноги — запрет передвижения в следующем ходу. Каждый выстрел +0.5 Тревоги, при попадании +1.", "category": "weapon", "count": 1},

        # ================== ОРУЖИЕ — ТЯЖЁЛОЕ ==================
        {"name": "Кремневая винтовка Дугина", "desc": "Попадание 12, эффективная дальность 5, макс 10, урон д10. 1 выстрел, накладывает ослепление 1 на использующего. Каждый выстрел +0.1 Тревоги, при попадании +1.", "category": "weapon", "count": 1},
        {"name": "Винтовка Бернара Б", "desc": "Попадание 10, эффективная дальность 6, макс 12, урон д14. Два выстрела за ход. Каждый выстрел +0.2 Тревоги, при попадании +0.5.", "category": "weapon", "count": 1},
        {"name": "Фастплаттер Модель 10", "desc": "Попадание 13, эффективная дальность 2, макс 4, урон д12, стреляет по конусу, -2 урона за каждую клетку между вами. Один выстрел за ход. Каждый выстрел +0.5 Тревоги, при попадании +1.", "category": "weapon", "count": 1},
        {"name": "Фастплаттер х89", "desc": "Попадание 14, эффективная дальность 1, макс 2, урон 5д4, стреляет по конусу. Один выстрел за ход. Каждый выстрел +0.5 Тревоги, при попадании +1.", "category": "weapon", "count": 1},
        {"name": "Гевехр", "desc": "Попадание 15, эффективная дальность 5, макс 10, урон д6. Очереди из 8 пуль, одиночные или непрерывный огонь (успех 18 если основное оружие, скорость вдвое меньше, нужен ход на установку). Каждый выстрел +0.1 Тревоги, при попадании +0.2.", "category": "weapon", "count": 1},
        {"name": "Самопальная винтовка", "desc": "Попадание 15, эффективная дальность 4, макс 8. До 2 выстрелов за ход. Каждый выстрел +0.2 Тревоги, при попадании +1.", "category": "weapon", "count": 1},

        # ================== ОРУЖИЕ — БЛИЖНЕГО БОЯ ==================
        {"name": "Армейский нож", "desc": "Попадание 8, урон д6. До 3 ударов за ход. Каждое попадание +0.5 Тревоги.", "category": "weapon", "count": 1},
        {"name": "Лом (оружие)", "desc": "Попадание 5, урон д6. Один удар, не увеличивает Тревогу.", "category": "weapon", "count": 1},
        {"name": "Полицейская дубинка", "desc": "Попадание 8, урон 0. До 2 ударов за ход. Каждое попадание -1 к Боевому духу.", "category": "weapon", "count": 1},

        # ================== ИНСТРУМЕНТЫ ==================
        {"name": "Лом (инструмент)", "desc": "+3 к результату броска на взлом или другие физические воздействия на предметы.", "category": "other", "count": 1},
        {"name": "Кусачки", "desc": "+3 к результату броска на технику, починку и т.д.", "category": "other", "count": 1},
        {"name": "Фонарик", "desc": "Позволяет видеть определённую область в темноте, нивелируя помеху.", "category": "other", "count": 1},
        {"name": "Бинокль", "desc": "Увеличивает эффективную дальность на 2 клетки, максимальную на 4. Пригодится для наведения.", "category": "other", "count": 1},
        {"name": "Пила", "desc": "+3 к результату броска при взаимодействии с деревянными предметами, их починке и разбору.", "category": "other", "count": 1},
        {"name": "Электрогазосварка", "desc": "+3 к результату броска при взаимодействии с металлическими предметами, их починке и разбору. Можно что-то заварить или разварить.", "category": "other", "count": 1},
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


# ================================================================
#                    СОХРАНЕНИЕ / ЗАГРУЗКА (REST)
# ================================================================
async def save_state():
    if not r:
        return
    async with save_lock:
        try:
            await r.set("dnd:clock_state", json.dumps(clock_state))
            await r.set("dnd:players_data", json.dumps(players_data, ensure_ascii=False))
            await r.set("dnd:base_inventory", json.dumps(base_inventory, ensure_ascii=False))
            await r.set("dnd:base_royals", json.dumps(base_royals))
            await r.set("dnd:item_templates", json.dumps(item_templates, ensure_ascii=False))
            await r.set("dnd:pending_trades", json.dumps(pending_trades, ensure_ascii=False))
        except Exception as e:
            print(f"[save_state] Ошибка: {e}", flush=True)


async def load_state():
    global base_royals
    if not r:
        print("[load_state] REDIS не подключён", flush=True)
        return
    try:
        d = await r.get("dnd:clock_state")
        if d: clock_state.update(json.loads(d))

        d = await r.get("dnd:players_data")
        if d:
            players_data.clear()
            players_data.update(json.loads(d))

        d = await r.get("dnd:base_inventory")
        if d:
            base_inventory.clear()
            base_inventory.extend(json.loads(d))

        d = await r.get("dnd:base_royals")
        if d: base_royals = json.loads(d)

        # ---- Заготовки: правильная инициализация с флагом ----
        d = await r.get("dnd:item_templates")
        loaded_templates = []
        if d:
            try:
                loaded_templates = json.loads(d)
            except Exception:
                loaded_templates = []

        if loaded_templates:
            item_templates.clear()
            item_templates.extend(loaded_templates)
        else:
            flag = await r.get("dnd:templates_initialized")
            if not flag:
                item_templates.clear()
                item_templates.extend(get_default_templates())
                await r.set("dnd:item_templates",
                            json.dumps(item_templates, ensure_ascii=False))
                await r.set("dnd:templates_initialized", "1")
                print(f"[load_state] Инициализированы дефолтные заготовки: "
                      f"{len(item_templates)}", flush=True)
            else:
                item_templates.clear()
                print("[load_state] Заготовки пусты (пользователь удалил всё)", flush=True)

        d = await r.get("dnd:pending_trades")
        if d:
            pending_trades.clear()
            pending_trades.extend(json.loads(d))

        print(f"[load_state] Загружено: {len(players_data)} игроков, "
              f"{len(base_inventory)} предметов, {len(item_templates)} заготовок", flush=True)
    except Exception as e:
        print(f"[load_state] Ошибка: {e}", flush=True)
        traceback.print_exc()


async def autosave_loop():
    while True:
        await asyncio.sleep(15)
        await save_state()


# ================================================================
#                       ХЕЛПЕРЫ
# ================================================================
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


# ================================================================
#                       BROADCAST
# ================================================================
async def broadcast_players():
    if clients:
        websockets.broadcast(clients, json.dumps({"type": "sync_players", "playersData": players_data}))

async def broadcast_social():
    if clients:
        websockets.broadcast(clients, json.dumps({
            "type": "sync_social",
            "baseInventory": base_inventory,
            "baseRoyals": base_royals,
            "itemTemplates": item_templates,
            "pendingTrades": pending_trades
        }))

async def broadcast_event(kind, data):
    if clients:
        websockets.broadcast(clients, json.dumps({"type": "event", "kind": kind, "data": data}))


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
                target, part_id, new_hp, new_def, new_max = data.get("target"), data.get("part"), data.get("hp"), data.get("def", 0), data.get("maxHp")
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
                                bucket[i] = merged; break
                        await broadcast_players()

            elif msg_type == "transfer_item":
                src, dst, data_type, item_id, count = data.get("from"), data.get("to"), data.get("dataType", "inventory"), data.get("itemId"), data.get("count", 1)
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
                            base_inventory.pop(idx); break
                    await broadcast_social()

            elif msg_type == "base_put":
                src, item_id, count = data.get("from"), data.get("itemId"), data.get("count", 1)
                if src and item_id:
                    ensure_player_shape(src)
                    bucket = players_data[src].get("inventory", [])
                    res = take_from_stack(bucket, item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(base_inventory, snapshot, taken)
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_put", {"from": src, "item": snapshot, "count": taken})

            elif msg_type == "base_take":
                player, base_item_id, count = data.get("player"), data.get("baseItemId"), data.get("count", 1)
                if player and base_item_id:
                    ensure_player_shape(player)
                    res = take_from_stack(base_inventory, base_item_id, count)
                    if res:
                        snapshot, taken = res
                        add_item_to_bucket(players_data[player].setdefault("inventory", []), snapshot, taken)
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_take", {"player": player, "item": snapshot, "count": taken})

            elif msg_type == "base_royals_put":
                player, amount = data.get("player"), max(0, safe_int(data.get("amount"), 0))
                if player and amount > 0:
                    ensure_player_shape(player)
                    have = int(players_data[player].get("royals", 0) or 0)
                    if have >= amount:
                        players_data[player]["royals"] = have - amount
                        base_royals += amount
                        await broadcast_players(); await broadcast_social()
                        await broadcast_event("base_royals_put", {"player": player, "amount": amount})

            elif msg_type == "base_royals_take":
                player, amount = data.get("player"), max(0, safe_int(data.get("amount"), 0))
                if player and amount > 0 and base_royals >= amount:
                    ensure_player_shape(player)
                    base_royals -= amount
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
                src, dst, from_type, to_type = data.get("from"), data.get("to"), data.get("fromType", "item"), data.get("toType", "item")
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


# ================================================================
#                       ЗАПУСК
# ================================================================
async def main():
    global r
    port = int(os.environ.get("PORT", 8765))

    print("[main] ========== STARTUP (REST API) ==========", flush=True)
    print(f"[main] PORT={port}", flush=True)
    print(f"[main] UPSTASH_REDIS_REST_URL is set: {bool(UPSTASH_REDIS_REST_URL)}", flush=True)
    print(f"[main] UPSTASH_REDIS_REST_TOKEN is set: {bool(UPSTASH_REDIS_REST_TOKEN)}", flush=True)

    if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
        try:
            print("[main] Подключаемся к Upstash через REST API...", flush=True)
            r = redis_async.Redis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)

            test_val = f"startup_ok_{int(time.time())}"
            await r.set("dnd:startup_test", test_val)
            readback = await r.get("dnd:startup_test")
            print(f"[main] Тестовая запись в Redis: {readback}", flush=True)

            await load_state()
            print("[main] load_state() завершён", flush=True)

        except Exception as e:
            print(f"[main] ОШИБКА Redis REST: {e}", flush=True)
            traceback.print_exc()
            r = None
    else:
        print("[main] Переменные UPSTASH_REDIS_REST_URL/TOKEN не заданы!", flush=True)

    asyncio.create_task(autosave_loop())
    print("[main] autosave_loop запущен", flush=True)

    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"[main] Server started on port {port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
