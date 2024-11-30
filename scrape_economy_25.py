import os
import pathlib
import sqlite3 as sqlite
import argparse
import sys
import copy
import sqlparse
import xmltodict

def tryint(text):
    if text:
        try:
            conv = int(text)
        except ValueError:
            conv = text
    else:
        conv = text
    return conv


def tryfloat(text):
    if text:
        try:
            conv = float(text)
        except ValueError:
            conv = text
    else:
        conv = text
    return conv


def trybool(text):
    if text:
        if text == 'true':
            conv = True
        elif text == 'false':
            conv = False
        else:
            conv = None
    else:
        conv = None
    return conv

def nested_set(dic, keys, value):
    for key in keys[:-1]:
        if not isinstance(dic[key], dict):
            v = dic[key]
            dic[key] = {'#text': v}
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value


def get_parent(game_dir, child, parent_link, key):
    parent_f = os.path.join(game_dir, parent_link.get('@xmlFilename').replace('$', ''))
    with open(parent_f, 'r', encoding='utf8') as f:
        data2 = f.read()
    d2 = xmltodict.parse(data2)
    parent = d2.get(key)
    for k in child.keys():
        parent[k] = child[k]
    child_attrs = parent_link.get('attributes')
    if child_attrs:
        set_list = child_attrs.get('set')
        if isinstance(set_list, dict):
            set_list = [set_list]
        for s in set_list:
            path_keys = s.get('@path').replace('#', '.@').split('.')[1:]
            value = s.get('@value')
            nested_set(dic=parent, keys=path_keys, value=value)
    return parent


def create_db(db_path=':memory:'):
    con = sqlite.connect(db_path)
    con.row_factory = sqlite.Row
    c = con.cursor()
    c.execute("PRAGMA foreign_keys = ON;")
    table_sql = [
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS prod_point (",
            "    id TEXT, type TEXT, name TEXT, price REAL, shared_throughput BOOLEAN,",
            "    PRIMARY KEY (id, type)"
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS production (",
            "    point_id TEXT, point_type TEXT, id TEXT, name TEXT, cycles_hour INTEGER,",
            "    cost_hour REAL,",
            "    PRIMARY KEY (point_id, point_type, id)",
            "    FOREIGN KEY(point_id, point_type) REFERENCES prod_point(id, type)",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS prod_fill (",
            "   point_id TEXT, point_type TEXT, prod_id TEXT, fill_type TEXT, direction TEXT,",
            "   amount REAL, capacity INTEGER, sell_direct BOOLEAN,",
            "   PRIMARY KEY (point_id, point_type, prod_id, fill_type),",
            "   FOREIGN KEY(point_id, point_type, prod_id)",
            "     REFERENCES production(point_id, point_type, id)",
            ");",
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS fruit (",
            "   name TEXT PRIMARY KEY, seed_rate REAL, seed_available BOOLEAN,",
            "   seed_needs_rolling BOOLEAN, liter_m2 REAL, chopper_type TEXT, bee_bonus REAL,",
            "   windrow_out TEXT, windrow_liter_m2 REAL, windrow_cut_fill TEXT,",
            "   windrow_cut_factor REAL, growth_resets_spray BOOLEAN, growth_require_lime BOOLEAN,",
            "   soil_lowdensity_req BOOLEAN, soil_increases_density BOOLEAN,",
            "   soil_consumes_lime BOOLEAN, soil_start_spraylevel INTEGER,",
            "   cultivation_allowed BOOLEAN, mulch_chopper_type TEXT",
            ");",
        )),
        "CREATE TABLE IF NOT EXISTS fruit_category (name TEXT PRIMARY KEY, fruit_types TEXT);",
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS fill (",
            "   name TEXT PRIMARY KEY, title TEXT, show BOOLEAN, unit TEXT, mass_l REAL,",
            "   price_l REAL",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS fill_factor (",
            "   name TEXT, period INTEGER, value REAL,",
            "   PRIMARY KEY (name, period),",
            "   FOREIGN KEY(name) REFERENCES fill(name)"
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS type_convert (type TEXT, name TEXT, input TEXT,",
            "   output TEXT, factor REAL,"
            "   PRIMARY KEY (type, name, input));"
        )),
        "CREATE TABLE IF NOT EXISTS fill_category (name TEXT PRIMARY KEY, fill_types TEXT);",
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal (",
            "   subtype TEXT PRIMARY KEY, type TEXT, reprod_agemin_mo INTEGER,",
            "   reprod_duration_mo INTEGER, reprod_healthmin REAL",
            ");",
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_price (",
            "   subtype TEXT, type TEXT, price_type TEXT, age_mo INTEGER, price_unit REAL, ",
            "   PRIMARY KEY (subtype, price_type, age_mo),"
            "   FOREIGN KEY(subtype) REFERENCES animal(subtype)"
            ");",
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_fill (",
            "   subtype TEXT, type TEXT, fill_type TEXT, direction TEXT, age_mo INTEGER,",
            "   liter_day REAL, ",
            "   PRIMARY KEY (subtype, fill_type, direction, age_mo), "
            "   FOREIGN KEY(subtype) REFERENCES animal(subtype)"
            ");",
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_food (",
            "   type TEXT PRIMARY KEY, consumption TEXT",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_food_group (",
            "   type TEXT, title TEXT, prod_wgt REAL, eat_wgt REAL",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_food_fill (",
            "   type TEXT, title TEXT, fill_type TEXT,",
            "   PRIMARY KEY (type, title, fill_type)"
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_food_mix (",
            "   type TEXT, fill_type TEXT, fill_types TEXT, wgt REAL",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_food_recipe (",
            "   fill_type TEXT, name TEXT, title TEXT, fill_types TEXT, pct_min REAL, pct_max REAL",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_point (",
            "   id TEXT PRIMARY KEY, place_type TEXT, name TEXT, price REAL, upkeep_price REAL,",
            "   type TEXT, unit_max INTEGER, food_cap INTEGER, food_default TEXT,",
            "   pallet_fill TEXT, pallet_maxno INTEGER, water_auto BOOLEAN",
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal_capacity (",
            "   point_id TEXT, fill_type TEXT, capacity INTEGER,",
            "   PRIMARY KEY (point_id, fill_type)"
            ");"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS placeable (",
            "   id TEXT PRIMARY KEY, name TEXT, type TEXT, price REAL, price_upkeep_day REAL,",
            "   brand TEXT, category TEXT, capacity REAL, is_extension BOOLEAN, radius REAL,",
            "   liter_day REAL, fill_type TEXT, income_hour REAL",
            ");"
        ))
    ]
    for stmt in table_sql:
        # print(stmt)
        try:
            c.execute(stmt)
        except sqlite.OperationalError as error:
            print(stmt)
            raise error
    con.commit()
    return con


def get_prod(game_dir, con):
    c = con.cursor()
    point_sql = '\n'.join((
        "INSERT OR IGNORE INTO prod_point (id, type, name, price, shared_throughput) ",
        "VALUES (:id, :type, :name, :price, :shared_throughput);"
    ))
    prod_sql = '\n'.join((
        "INSERT OR IGNORE INTO production (point_id, point_type, id, name, cycles_hour, cost_hour)",
        "VALUES (:point_id, :point_type, :id, :name, :cycles_hour, :cost_hour);"
    ))
    fill_sql = '\n'.join((
        "INSERT OR IGNORE INTO prod_fill (point_id, point_type, prod_id, fill_type, direction,",
        "  amount, sell_direct) ",
        "VALUES (:point_id, :point_type, :prod_id, :fill_type, :direction, :amount, :sell_direct);"
    ))
    cap_sql = '\n'.join((
        "UPDATE prod_fill SET capacity = :capacity ",
        " WHERE point_id = :point_id AND point_type = :point_type AND prod_id = :prod_id",
        "   AND fill_type = :fill_type;"))

    dirs = []
    dirs.append(os.path.join(game_dir, "data", "placeables",  "brandless",
                             "productionPointsGeneric"))
    dirs.append(os.path.join(game_dir, "data", "placeables",  "brandless", "productionPointsSmall"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "brandless", "greenhouses",
                             "greenHouseGlass"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "brandless", "greenhouses",
                             "greenHouseMushrooms"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "brandless", "greenhouses",
                             "greenHousesTarp"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "brandless", "greenhouses",
                             "greenHousesSaplings"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "planET"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "mapUS", "cooperUS"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "mapUS", "cementFactoryUS"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "mapUS", "cannedPackagedFactoryUS"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "mapUS", "ropemakerUS"))

    xml_files = []
    for folder_path in dirs:
        for p in pathlib.Path(folder_path).rglob("*"):
            if os.path.splitext(p)[1] == '.xml':
                xml_files.append(p)

    xml_files.append(pathlib.Path(game_dir, "data", "placeables", "mapUS", "playgroundMaker",
                                  "playgroundMakerHall.xml"))
    xml_files.append(pathlib.Path(game_dir, "data", "placeables", "mapEU", "pianoFactory",
                                  "pianoFactoryPlaceable.xml"))
    xml_files.append(pathlib.Path(game_dir, "data", "placeables", "mapEU", "potatoProcessingPlant",
                                  "potatoProcessingPlantPlaceable.xml"))
    xml_files.append(pathlib.Path(game_dir, "data", "placeables", "mapEU", "preservedFoodFactory",
                                  "preservedFoodFactoryPlaceable.xml"))
    xml_files.append(pathlib.Path(game_dir, "data", "placeables", "mapEU", "soupFactory",
                                  "soupFactoryPlaceable.xml"))
    xml_files = sorted(xml_files)

    point_out = []
    prod_out = []
    fill_out = []
    cap_out = []

    for file in xml_files:
        point_dict = {}
        file_name = os.path.basename(file)
        point_id = os.path.splitext(file_name)[0]
        point_type = os.path.basename(os.path.dirname(os.path.dirname(file)))
        with open(file, 'r', encoding='utf8') as f:
            data = f.read()
        d = xmltodict.parse(data)

        # production point
        point_dict['id'] = point_id
        point_dict['type'] = point_type
        place = d.get('placeable')
        if place:
            store = place.get('storeData')
            if store:
                store_name = store.get('name')
                if isinstance(store_name, dict):
                    name_params = store_name.get('@params')
                    name_text = store_name.get('#text')
                    point_dict['name'] = name_text % tuple(name_params.split('|'))
                else:
                    point_dict['name'] = store_name
                point_dict['price'] = tryint(store.get('price'))
                # lifetime = int(store.get('lifetime'))
                prod_point = place.get('productionPoint')
                if prod_point:
                    prods = prod_point.get('productions')
                    if prods.get('@sharedThroughputCapacity'):
                        point_dict['shared_throughput'] = \
                          trybool(prods.get('@sharedThroughputCapacity'))
                    else:
                        point_dict['shared_throughput'] = True
                    point_out.append(point_dict)

                    # individual productions
                    prod_list = prods.get('production')
                    if isinstance(prod_list, dict):
                        prod_list = [prod_list]
                    for p in prod_list:
                        prod_dict = {}
                        prod_dict['point_id'] = point_id
                        prod_dict['point_type'] = point_type
                        prod_id = p.get('@id')
                        prod_dict['id'] = prod_id
                        prod_name = p.get('@name')
                        prod_params = p.get('@params')
                        if prod_params:
                            prod_name = prod_name % tuple(prod_params.split('|'))
                        prod_dict['name'] = prod_name
                        prod_dict['cycles_hour'] = tryint(p.get('@cyclesPerHour'))
                        prod_dict['cost_hour'] = tryfloat(p.get('@costsPerActiveHour'))
                        prod_out.append(prod_dict)

                        # inputs
                        inputs = p.get('inputs')
                        input_list = inputs.get('input')
                        if isinstance(input_list, dict):
                            input_list = [input_list]
                        for i in input_list:
                            input_dict = {}
                            input_dict['point_id'] = point_id
                            input_dict['point_type'] = point_type
                            input_dict['prod_id'] = prod_id
                            input_dict['fill_type'] = i.get('@fillType')
                            input_dict['direction'] = 'in'
                            input_dict['amount'] = tryfloat(i.get('@amount'))
                            input_dict['sell_direct'] = trybool(i.get('@sellDirectly'))
                            fill_out.append(input_dict)

                        # outputs
                        outputs = p.get('outputs')
                        output_list = outputs.get('output')
                        if isinstance(output_list, dict):
                            output_list = [output_list]
                        for o in output_list:
                            output_dict = {}
                            output_dict['point_id'] = point_id
                            output_dict['point_type'] = point_type
                            output_dict['prod_id'] = prod_id
                            output_dict['fill_type'] = o.get('@fillType')
                            output_dict['direction'] = 'out'
                            output_dict['amount'] = tryfloat(o.get('@amount'))
                            output_dict['sell_direct'] = trybool(o.get('@sellDirectly'))
                            if output_dict['sell_direct'] is None:
                                output_dict['sell_direct'] = False
                            fill_out.append(output_dict)

                        # capacity
                        storage = prod_point.get('storage')
                        cap_list = storage.get('capacity')
                        if isinstance(cap_list, dict):
                            cap_list = [cap_list]
                        for cap in cap_list:
                            cap_dict = {}
                            cap_dict['point_id'] = point_id
                            cap_dict['point_type'] = point_type
                            cap_dict['prod_id'] = prod_id
                            cap_dict['fill_type'] = cap.get('@fillType')
                            cap_dict['capacity'] = tryfloat(cap.get('@capacity'))
                            cap_out.append(cap_dict)
        c.executemany(point_sql, point_out)
        c.executemany(prod_sql, prod_out)
        c.executemany(fill_sql, fill_out)
        c.executemany(cap_sql, cap_out)
        con.commit()


def get_fruit(game_dir, con):
    fruit_sql = '\n'.join((
        "INSERT OR IGNORE INTO fruit (",
        "   name, seed_rate, seed_available, seed_needs_rolling, liter_m2, chopper_type,",
        "   bee_bonus, windrow_out, windrow_liter_m2, windrow_cut_fill, windrow_cut_factor,",
        "   growth_resets_spray, growth_require_lime, soil_lowdensity_req, soil_increases_density,",
        "   soil_consumes_lime, soil_start_spraylevel, cultivation_allowed, mulch_chopper_type)",
        "VALUES (:name, :seed_rate, :seed_available, :seed_needs_rolling, :liter_m2,",
        "   :chopper_type, :bee_bonus, :windrow_out, :windrow_liter_m2, :windrow_cut_fill,",
        "   :windrow_cut_factor, :growth_resets_spray, :growth_require_lime, :soil_lowdensity_req,",
        "   :soil_increases_density, :soil_consumes_lime, :soil_start_spraylevel,",
        "   :cultivation_allowed, :mulch_chopper_type);"
    ))

    fruit_cat_sql = ("INSERT OR IGNORE INTO fruit_category (name, fruit_types) "
                     "VALUES (:name, :text);")

    convert_sql = '\n'.join((
        "INSERT OR IGNORE INTO type_convert (type, name, input, output, factor) ",
        "VALUES (:type, :name, :input, :output, :factor);"
    ))

    c = con.cursor()
    map_dir = os.path.join(game_dir, "data", "maps")
    fruit_file = os.path.join(map_dir, "maps_fruitTypes.xml")
    with open(fruit_file, 'r', encoding='utf8') as f:
        data = f.read()
    d = xmltodict.parse(data)
    fruit_file_list = d.get('map').get('fruitTypes').get('fruitType')

    fruit_out = []
    for foliage in fruit_file_list:
        fruit_subf = os.path.join(game_dir, foliage.get('@filename').replace('$', ''))
        with open(fruit_subf, 'r', encoding='utf8') as f:
            data2 = f.read()
        d2 = xmltodict.parse(data2)
        fruit = d2.get('foliageType').get('fruitType')
        fruit_dict = {}
        fruit_dict['name'] = fruit.get('@name')

        seeding = fruit.get('seeding')
        if seeding:
            fruit_dict['seed_rate'] = tryfloat(seeding.get('@litersPerSqm'))
            fruit_dict['seed_available'] = trybool(seeding.get('@isAvailable'))
            fruit_dict['seed_needs_rolling'] = trybool(seeding.get('@needsRolling'))
        else:
            fruit_dict['seed_rate'] = None
            fruit_dict['seed_available'] = None
            fruit_dict['seed_needsrolling'] = None

        harvest = fruit.get('harvest')
        if harvest:
            fruit_dict['liter_m2'] = tryfloat(harvest.get('@litersPerSqm'))
            fruit_dict['chopper_type'] = tryfloat(harvest.get('@chopperType'))
            fruit_dict['bee_bonus'] = tryfloat(harvest.get('@beeYieldBonusPercentage'))
            if fruit_dict['bee_bonus'] is None:
                fruit_dict['bee_bonus'] = 0
        else:
            fruit_dict['liter_m2'] = None
            fruit_dict['chopper_type'] = None
            fruit_dict['bee_bonus'] = None

        windrow = fruit.get('windrow')
        if windrow:
            fruit_dict['windrow_out'] = windrow.get('@fillType')
            fruit_dict['windrow_liter_m2'] = tryfloat(windrow.get('@litersPerSqm'))
            fruit_dict['windrow_cut_fill'] = windrow.get('@cutFillType')
            fruit_dict['windrow_cut_factor'] = tryfloat(windrow.get('@windrowCutFactor'))
        else:
            fruit_dict['windrow_out'] = None
            fruit_dict['windrow_liter_m2'] = None
            fruit_dict['windrow_cut_fill'] = None
            fruit_dict['windrow_cut_factor'] = None

        growth = fruit.get('growth')
        if growth:
            fruit_dict['growth_resets_spray'] = trybool(growth.get('@resetsSpray'))
            fruit_dict['growth_require_lime'] = trybool(growth.get('@growthRequiresLime'))
        else:
            fruit_dict['growth_resets_spray'] = None
            fruit_dict['growth_require_lime'] = None

        soil = fruit.get('soil')
        if soil:
            fruit_dict['soil_lowdensity_req'] = trybool(soil.get('@lowDensityRequired'))
            fruit_dict['soil_increases_density'] = trybool(soil.get('@increasesDensity'))
            fruit_dict['soil_consumes_lime'] = trybool(soil.get('@consumesLime'))
            fruit_dict['soil_start_spraylevel'] = tryint(soil.get('@startSprayLevel'))
        else:
            fruit_dict['soil_lowdensity_req'] = None
            fruit_dict['soil_increases_density'] = None
            fruit_dict['soil_consumes_lime'] = None
            fruit_dict['soil_start_spraylevel'] = None

        cult = fruit.get('cultivation')
        if cult:
            fruit_dict['cultivation_allowed'] = trybool(cult.get('@isAllowed'))
        else:
            fruit_dict['cultivation_allowed'] = None

        mulch = fruit.get('mulcher')
        if mulch:
            fruit_dict['mulch_chopper_type'] = mulch.get('@chopperType')
        else:
            fruit_dict['mulch_chopper_type'] = None

        fruit_out.append(fruit_dict)
    c.executemany(fruit_sql, fruit_out)
    con.commit()

    # fruit categories
    fruit_cats = d.get('map').get('fruitTypeCategories').get('fruitTypeCategory')
    for l in fruit_cats:
        l['name'] = l.pop('@name')
        l['text'] = l.pop('#text')
    c.executemany(fruit_cat_sql, fruit_cats)
    con.commit()

    # fruit conversions
    convert_list = d.get('map').get('fruitTypeConverters').get('fruitTypeConverter')
    convert_out = []
    convert_type = 'fruit'
    for convert in convert_list:
        convert_name = convert.get('@name')
        converter_list = convert.get('converter')
        if isinstance(converter_list, dict):
            converter_list = [converter_list]
        for l in converter_list:
            convert_dict = {}
            convert_dict['type'] = convert_type
            convert_dict['name'] = convert_name
            convert_dict['input'] = l.get('@from')
            convert_dict['output'] = l.get('@to')
            convert_dict['factor'] = tryfloat(l.get('@factor'))
            convert_out.append(convert_dict)
    c.executemany(convert_sql, convert_out)
    con.commit()



def get_fill(game_dir, con):
    convert_sql = '\n'.join((
        "INSERT OR IGNORE INTO type_convert (type, name, input, output, factor) ",
        "VALUES (:type, :name, :input, :output, :factor);"
    ))
    fill_sql = '\n'.join((
        "INSERT OR IGNORE INTO fill (name, title, show, unit, mass_l, price_l) ",
        "VALUES (:name, :title, :show, :unit, :mass_l, :price_l);"
    ))
    factor_sql = ("INSERT OR IGNORE INTO fill_factor (name, period, value) VALUES "
                  "(:name, :period, :value);")

    cat_sql = "INSERT OR IGNORE INTO fill_category (name, fill_types) VALUES (:name, :text);"
    c = con.cursor()
    map_dir = os.path.join(game_dir, "data", "maps")
    fruit_file = os.path.join(map_dir, "maps_fillTypes.xml")
    with open(fruit_file, 'r', encoding='utf8') as f:
        data = f.read()
    d = xmltodict.parse(data)

    # fills
    fill_list = d.get('map').get('fillTypes').get('fillType')
    if isinstance(fill_list, dict):
        fill_list = [fill_list]
    fill_out = []
    factor_out = []
    for fill in fill_list:
        fill_dict = {}
        fill_name = fill.get('@name')
        fill_dict['name'] = fill_name
        fill_dict['title'] = fill.get('@title')
        fill_dict['show'] = trybool(fill.get('@showOnPriceTable'))
        fill_dict['unit'] = fill.get('@unitShort')
        if fill.get('physics') is not None:
            fill_dict['mass_l'] = tryfloat(fill.get('physics').get('@massPerLiter'))
        else:
            fill_dict['mass_l'] = None
        economy = fill.get('economy')
        if economy is not None:
            fill_dict['price_l'] = tryfloat(economy.get('@pricePerLiter'))

        # fill factors
            factors = economy.get('factors')
            if factors is not None:
                factor_list = factors.get('factor')
                for factor in factor_list:
                    factor_dict = {}
                    factor_dict['name'] = fill_name
                    factor_dict['period'] = tryint(factor.get('@period'))
                    factor_dict['value'] = tryfloat(factor.get('@value'))
                    factor_out.append(factor_dict)
        else:
            fill_dict['price_l'] = None
        fill_out.append(fill_dict)
    c.executemany(fill_sql, fill_out)
    c.executemany(factor_sql, factor_out)

    # fill conversions
    convert_list = d.get('map').get('fillTypeConverters').get('fillTypeConverter')
    if isinstance(convert_list, dict):
        convert_list = [convert_list]
    convert_out = []
    convert_type = 'fill'
    for convert in convert_list:
        convert_name = convert.get('@name')
        converter_list = convert.get('converter')
        if isinstance(converter_list, dict):
            converter_list = [converter_list]
        for l in converter_list:
            convert_dict = {}
            convert_dict['type'] = convert_type
            convert_dict['name'] = convert_name
            convert_dict['input'] = l.get('@from')
            convert_dict['output'] = l.get('@to')
            convert_dict['factor'] = tryfloat(l.get('@factor'))
            convert_out.append(convert_dict)
    c.executemany(convert_sql, convert_out)
    con.commit()

    # fill categories
    cat_list = d.get('map').get('fillTypeCategories').get('fillTypeCategory')
    for l in cat_list:
        l['name'] = l.pop('@name')
        l['text'] = l.pop('#text')

    c.executemany(cat_sql, cat_list)
    con.commit()


def get_animals(dataS_dir, con):
    animal_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal (type, subtype, reprod_agemin_mo, reprod_duration_mo,",
        "reprod_healthmin) VALUES (:type, :subtype, :reprod_agemin_mo, :reprod_duration_mo,",
        ":reprod_healthmin);"
    ))
    price_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_price (subtype, type, price_type, age_mo, price_unit)",
        "VALUES (:subtype, :type, :price_type, :age_mo, :price_unit);"
    ))
    # we must use REPLACE instead of IGNORE due to cow milk output having two values for the 12
    # month age class this is probably because the game creates age ranges (e.g. 0-12, 12+)
    # from these values, but we won't do that
    fill_sql = '\n'.join((
        "INSERT OR REPLACE INTO animal_fill (subtype, type, fill_type, direction, age_mo,",
        "liter_day) VALUES (:subtype, :type, :fill_type, :direction, :age_mo, :liter_day);"
    ))
    c = con.cursor()
    animal_dir = os.path.join(dataS_dir, "character")
    animal_file = os.path.join(animal_dir, "animals.xml")
    with open(animal_file, 'r', encoding='utf8') as f:
        data = f.read()
    d = xmltodict.parse(data)
    animal_list = d.get('animals').get('animal')
    if isinstance(animal_list, dict):
        animal_list = [animal_list]

    animal_out = []
    price_out = []
    fill_out = []
    for a in animal_list:
        animal_type = a.get('@type')
        sub_list = a.get('subType')
        if isinstance(sub_list, dict):
            sub_list = [sub_list]
        for sub in sub_list:
            sub_dict = {}
            sub_dict['type'] = animal_type
            sub_dict['subtype'] = sub.get('@subType')
            reprod = sub.get('reproduction')
            sub_dict['reprod_agemin_mo'] = tryint(reprod.get('@minAgeMonth'))
            sub_dict['reprod_duration_mo'] = tryint(reprod.get('@durationMonth'))
            sub_dict['reprod_healthmin'] = tryfloat(reprod.get('@minHealthFactor'))
            animal_out.append(sub_dict)

            # prices
            buy = sub.get('buyPrice')
            if buy:
                buy_price = buy.get('key')
                if isinstance(buy_price, dict):
                    buy_price = [buy_price]
                for key in buy_price:
                    buy_dict = {}
                    buy_dict['type'] = animal_type
                    buy_dict['subtype'] = sub.get('@subType')
                    buy_dict['price_type'] = 'buy'
                    buy_dict['age_mo'] = tryint(key.get('@ageMonth'))
                    buy_dict['price_unit'] = tryfloat(key.get('@value'))
                    price_out.append(buy_dict)

            sell = sub.get('sellPrice')
            if sell:
                sell_price = sell.get('key')
                if isinstance(sell_price, dict):
                    sell_price = [sell_price]
                for key in sell_price:
                    sell_dict = {}
                    sell_dict['type'] = animal_type
                    sell_dict['subtype'] = sub.get('@subType')
                    sell_dict['price_type'] = 'sell'
                    sell_dict['age_mo'] = tryint(key.get('@ageMonth'))
                    sell_dict['price_unit'] = tryfloat(key.get('@value'))
                    price_out.append(sell_dict)

            trans = sub.get('transportPrice')
            if trans:
                trans_price = trans.get('key')
                if isinstance(trans_price, dict):
                    trans_price = [trans_price]
                for key in trans_price:
                    trans_dict = {}
                    trans_dict['type'] = animal_type
                    trans_dict['subtype'] = sub.get('@subType')
                    trans_dict['price_type'] = 'transport'
                    trans_dict['age_mo'] = tryint(key.get('@ageMonth'))
                    trans_dict['price_unit'] = tryfloat(key.get('@value'))
                    price_out.append(trans_dict)

            iput = sub.get('input')
            if iput:
                for k, v in iput.items():
                    fill_type = k
                    key_list = v.get('key')
                    if isinstance(key_list, dict):
                        key_list = [key_list]
                    for key in key_list:
                        in_dict = {}
                        in_dict['type'] = animal_type
                        in_dict['subtype'] = sub.get('@subType')
                        in_dict['fill_type'] = fill_type
                        in_dict['direction'] = 'in'
                        in_dict['age_mo'] = tryint(key.get('@ageMonth'))
                        in_dict['liter_day'] = tryfloat(key.get('@value'))
                        # print(in_dict)
                        fill_out.append(in_dict)

            output = sub.get('output')
            if output:
                for k, v in output.items():
                    fill_type = k
                    key_list = v.get('key')
                    if isinstance(key_list, dict):
                        key_list = [key_list]
                    for key in key_list:
                        out_dict = {}
                        out_dict['type'] = animal_type
                        out_dict['subtype'] = sub.get('@subType')
                        out_dict['fill_type'] = fill_type
                        out_dict['direction'] = 'out'
                        out_dict['age_mo'] = tryint(key.get('@ageMonth'))
                        out_dict['liter_day'] = tryfloat(key.get('@value'))
                        # print(out_dict)
                        fill_out.append(out_dict)

    c.executemany(animal_sql, animal_out)
    c.executemany(price_sql, price_out)
    c.executemany(fill_sql, fill_out)
    con.commit()


def get_animal_food(dataS_dir, con):
    animal_sql = ("INSERT OR IGNORE INTO animal_food (type, consumption) VALUES (:type, "
                  ":consumption);")
    group_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_food_group (type, title, prod_wgt, eat_wgt) ",
        "VALUES (:type, :title, :prod_wgt, :eat_wgt);"
    ))
    fill_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_food_fill (type, title, fill_type) ",
        "VALUES (:type, :title, :fill_type);"
    ))
    mix_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_food_mix (type, fill_type, fill_types, wgt) ",
        "VALUES (:type, :fill_type, :fill_types, :wgt);"
    ))
    rec_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_food_recipe (fill_type, name, title, fill_types, pct_min,",
        "pct_max) VALUES (:fill_type, :name, :title, :fill_types, :pct_min, :pct_max);"
    ))
    c = con.cursor()
    animal_dir = os.path.join(dataS_dir, "character")
    food_file = os.path.join(animal_dir, "animalFood.xml")
    with open(food_file, 'r', encoding='utf8') as f:
        data = f.read()
    d = xmltodict.parse(data)
    animal_food = d.get('animalFood')
    animal_list = animal_food.get('animals').get('animal')
    if isinstance(animal_list, dict):
        animal_list = [animal_list]

    animal_out = []
    group_out = []
    fill_out = []
    for a in animal_list:
        animal_dict = {}
        animal_type = a.get('@animalType')
        animal_dict['type'] = animal_type
        animal_dict['consumption'] = a.get('@consumptionType')
        animal_out.append(animal_dict)

        # food groups
        group_list = a.get('foodGroup')
        if isinstance(group_list, dict):
            group_list = [group_list]
        for group in group_list:
            group_dict = {}
            group_dict['type'] = animal_type
            group_dict['prod_wgt'] = tryfloat(group.get('@productionWeight'))
            group_dict['eat_wgt'] = tryfloat(group.get('@eatWeight'))
            title = tryfloat(group.get('@title'))
            group_dict['title'] = title
            # print(group_dict)
            group_out.append(group_dict)

            fill_types = group.get('@fillTypes')
            if fill_types:
                fill_list = fill_types.split(' ')
                for fill in fill_list:
                    fill_dict = {}
                    fill_dict['type'] = animal_type
                    fill_dict['title'] = title
                    fill_dict['fill_type'] = fill
                    fill_out.append(fill_dict)

    # mixtures
    mix_out = []
    mix_list = animal_food.get('mixtures').get('mixture')
    if isinstance(mix_list, dict):
        mix_list = [mix_list]
    for m in mix_list:
        mix_type = m.get('@animalType')
        mix_fill = m.get('@fillType')
        # ingredients
        in_list = m.get('ingredient')
        if isinstance(in_list, dict):
            in_list = [in_list]
        for i in in_list:
            in_dict = {}
            in_dict['type'] = mix_type
            in_dict['fill_type'] = mix_fill
            in_dict['fill_types'] = i.get('@fillTypes')
            in_dict['wgt'] = tryfloat(i.get('@weight'))
            mix_out.append(in_dict)


    # recipes
    rec_out = []
    rec_list = animal_food.get('recipes').get('recipe')
    if isinstance(rec_list, dict):
        rec_list = [rec_list]
    for r in rec_list:
        rec_fill = r.get('@fillType')
        rin_list = r.get('ingredient')
        if isinstance(rin_list, dict):
            rin_list = [rin_list]
        for ri in rin_list:
            rin_dict = {}
            rin_dict['fill_type'] = rec_fill
            rin_dict['name'] = ri.get('@name')
            rin_dict['title'] = ri.get('@title')
            rin_dict['fill_types'] = ri.get('@fillTypes')
            rin_dict['pct_min'] = tryfloat(ri.get('@minPercentage'))/100
            rin_dict['pct_max'] = tryfloat(ri.get('@maxPercentage'))/100
            rec_out.append(rin_dict)
    c.executemany(animal_sql, animal_out)
    c.executemany(group_sql, group_out)
    c.executemany(fill_sql, fill_out)
    c.executemany(mix_sql, mix_out)
    c.executemany(rec_sql, rec_out)
    con.commit()


def get_animal_pen(game_dir, con):
    point_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_point (id, place_type, name, price, upkeep_price, type,",
        "  unit_max, food_cap, food_default, pallet_fill, pallet_maxno, water_auto)",
        "VALUES (:id, :place_type, :name, :price, :upkeep_price, :type, :unit_max, :food_cap, ",
        "   :food_default, :pallet_fill, :pallet_maxno, :water_auto);"
    ))
    cap_sql = '\n'.join((
        "INSERT OR IGNORE INTO animal_capacity (point_id, fill_type, capacity)",
        "VALUES (:point_id, :fill_type, :capacity);"
    ))
    c = con.cursor()
    dirs = []
    dirs.append(os.path.join(game_dir, "data", "placeables", "brandless","animalHusbandries"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "chickenBarnBig"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "cowBarnBig"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "cowBarnMedium"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "horseBarnBig"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "horseBarnMedium"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "pigBarnBig"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "pigBarnMedium"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "sheepBarnBig"))
    dirs.append(os.path.join(game_dir, "data", "placeables", "rudolfHormann", "sheepBarnMedium"))

    point_out = []
    cap_out = []
    xml_files = []
    for folder_path in dirs:
        for p in pathlib.Path(folder_path).rglob("*"):
            if os.path.splitext(p)[1] == '.xml':
                xml_files.append(p)
    xml_files = sorted(xml_files)
    for file in xml_files:
        point_dict = {}
        file_name = os.path.basename(file)
        point_id = os.path.splitext(file_name)[0]
        with open(file, 'r', encoding='utf8') as f:
            data = f.read()
        d = xmltodict.parse(data)

        point_dict['id'] = point_id
        place = d.get('placeable')
        if place:
            point_dict['place_type'] = place.get('@type')
            store = place.get('storeData')
            if store:
                point_dict['name'] = store.get('name')
                point_dict['price'] = tryint(store.get('price'))
                point_dict['upkeep_price'] = tryint(store.get('dailyUpkeep'))
            husbandry = place.get('husbandry')
            if husbandry:
                storage = husbandry.get('storage')
                if storage:
                    cap_list = storage.get('capacity')
                    if isinstance(cap_list, dict):
                        cap_list = [cap_list]
                    for cap in cap_list:
                        cap_dict = {}
                        cap_dict['point_id'] = point_id
                        cap_dict['fill_type'] = cap.get('@fillType')
                        cap_dict['capacity'] = tryint(cap.get('@capacity'))
                        cap_out.append(cap_dict)

                animals = husbandry.get('animals')
                if animals:
                    point_dict['type'] = animals.get('@type')
                    point_dict['unit_max'] = tryint(animals.get('@maxNumAnimals'))
                else:
                    point_dict['type'] = None
                    point_dict['unit_max'] = None
                food = husbandry.get('food')
                if food:
                    point_dict['food_cap'] = tryint(food.get('@capacity'))
                    if food.get('foodPlane'):
                        point_dict['food_default'] = food.get('foodPlane')\
                                .get('@defaultFillType')
                    elif food.get('dynamicFoodPlane'):
                        point_dict['food_default'] = food.get('dynamicFoodPlane')\
                                .get('@defaultFillType')
                    else:
                        point_dict['food_default'] = None
                else:
                    point_dict['food_cap'] = None
                    point_dict['food_default'] = None
                pallets = husbandry.get('pallets')
                if pallets:
                    point_dict['pallet_fill'] = pallets.get('palletSpawner').get('@fillTypes')
                    point_dict['pallet_maxno'] = tryint(pallets.get('palletSpawner')\
                            .get('@maxNumPallets'))
                else:
                    point_dict['pallet_fill'] = None
                    point_dict['pallet_maxno'] = None
                water = husbandry.get('water')
                if water:
                    point_dict['water_auto'] = trybool(water.get('@automaticWaterSupply'))
                else:
                    point_dict['water_auto'] = None
                point_out.append(point_dict)

    c.executemany(point_sql, point_out)
    c.executemany(cap_sql, cap_out)
    con.commit()


def get_placeable(game_dir, con):
    place_sql = '\n'.join((
        "INSERT OR IGNORE INTO placeable (",
        "   id, name, type, price, price_upkeep_day, brand, category, capacity, is_extension,",
        "   radius, liter_day, fill_type, income_hour)",
        "VALUES (:id, :name, :type, :price, :price_upkeep_day, :brand, :category, :capacity,",
        "  :is_extension, :radius, :liter_day, :fill_type, :income_hour);"
    ))
    c = con.cursor()
    dirs = []
    brandless = os.path.join(game_dir, "data", "placeables", "brandless")
    dirs.append(pathlib.Path(brandless, "beeHives"))
    dirs.append(pathlib.Path(brandless, "manureHeap"))
    dirs.append(pathlib.Path(brandless, "riceField"))
    dirs.append(pathlib.Path(brandless, "electricityGenerators"))
    place_out = []
    xml_files = []
    for folder_path in dirs:
        for p in pathlib.Path(folder_path).rglob("*"):
            if os.path.splitext(p)[1] == '.xml':
                xml_files.append(p)
    xml_files = sorted(xml_files)
    for file in xml_files:
        point_dict = {}
        file_name = os.path.basename(file)
        point_id = os.path.splitext(file_name)[0]
        with open(file, 'r', encoding='utf8') as f:
            data = f.read()
        d = xmltodict.parse(data)

        # point
        point_dict['id'] = point_id
        place = d.get('placeable')
        if place:
            parent = place.get('parentFile')
            if parent is not None:
                place = get_parent(game_dir=game_dir, child=place, parent_link=parent,
                                   key='placeable')

            point_dict['type'] = place.get('@type')

            store = place.get('storeData')
            if store:
                store_name = store.get('name')
                if isinstance(store_name, dict):
                    name_params = store_name.get('@params')
                    name_text = store_name.get('#text')
                    point_dict['name'] = name_text % tuple(name_params.split('|'))
                else:
                    point_dict['name'] = store.get('name')
                point_dict['price'] = tryint(store.get('price'))
                point_dict['price_upkeep_day'] = store.get('dailyUpkeep')
                point_dict['brand'] = store.get('brand')
                point_dict['category'] = store.get('category')
                # here in case of multiple types of tags
                point_dict['radius'] = None
                point_dict['liter_day'] = None
                point_dict['capacity'] = None
                point_dict['is_extension'] = 0
                point_dict['fill_type'] = None
                point_dict['income_hour'] = None

            beehive = place.get('beehive')
            if beehive:
                point_dict['radius'] = tryfloat(beehive.get('@actionRadius'))
                point_dict['liter_day'] = tryfloat(beehive.get('@litersHoneyPerDay'))
                point_dict['fill_type'] = 'HONEY'

            manure_heap = place.get('manureHeap')
            if manure_heap:
                point_dict['capacity'] = tryfloat(manure_heap.get('@capacity'))
                point_dict['is_extension'] = trybool(manure_heap.get('@isExtension'))
                point_dict['fill_type'] = 'MANURE'

            solar_panels = place.get('solarPanels')
            if solar_panels:
                point_dict['income_hour'] = tryfloat(
                        solar_panels.get('solarPanelsConfigurations')\
                        .get('solarPanelsConfiguration')\
                        .get('@incomePerHour'))

            wind_turbine = place.get('windTurbine')
            if wind_turbine:
                point_dict['income_hour'] = tryfloat(wind_turbine.get('@incomePerHour'))

            place_out.append(point_dict)
    c.executemany(place_sql, place_out)
    con.commit()

def add_production_queries(con):
    c = con.cursor()
    with open('prod25.sql', 'r', encoding='utf8') as file:
        queries_str = file.read()
    queries = sqlparse.split(queries_str)
    for query in queries:
        c.execute(query)


if __name__ == '__main__':
    my_args = sys.argv[1:]
    parser = argparse.ArgumentParser(
                    prog='fs_scraper',
                    description='Scrapes Farming Simulator 25 game data folder(s) and '
                                'populates a sqlite database with results.',
                    epilog='Script finished.')

    parser.add_argument('game_dir', help='The base game path containing the `data` folder.')
    parser.add_argument('db_path',
                        help='The path at which to store the scraped contents database')

    parser.add_argument('-d', '--dataS_dir',
                        help='The path containing the dataS directory (unzipped and unencrypted'
                             ' of course).')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='Overwrite database if it already exists.')

    args = parser.parse_args(my_args)

    if os.path.isfile(args.db_path) and args.overwrite:
        os.remove(args.db_path)
    ocon = create_db(db_path=args.db_path)
    print("Scraping production...")
    get_prod(game_dir=args.game_dir, con=ocon)
    print("Scraping fruit...")
    get_fruit(game_dir=args.game_dir, con=ocon)
    print("Scraping fill...")
    get_fill(game_dir=args.game_dir, con=ocon)
    if args.dataS_dir is not None:
        print("Scraping animals...")
        get_animals(dataS_dir=args.dataS_dir, con=ocon)
        print("Scraping animal food...")
        get_animal_food(dataS_dir=args.dataS_dir, con=ocon)
    print("Scraping animal pens...")
    get_animal_pen(game_dir=args.game_dir, con=ocon)
    print("Scraping other placeables...")
    get_placeable(game_dir=args.game_dir, con=ocon)
    print("Adding production queries...")
    add_production_queries(con=ocon)
    ocon.close()
