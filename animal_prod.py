import sqlite3 as sqlite
import warnings
import copy
import math
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import os
from sklearn.linear_model import LinearRegression
from typing import Optional

matplotlib.use('GTK3Agg')
warnings.simplefilter(action='ignore', category=FutureWarning)
plt.ion()

class PointAnimal:
    def __init__(self, id: str, place_type: str, name: str, price: float, upkeep_price: float, type: str,
                 unit_max: int, food_cap: int, food_default: str, animal, animal_choices: list, capacity: list,
                 pallet_fill: Optional[str] = None, pallet_maxno: Optional[int] = None,
                 water_auto: Optional[bool] = None, fill: Optional[dict] = None, auto_buy: Optional[bool] = False,
                 auto_sell: Optional[bool] = False, animal_autosell: Optional[str] = 'none',
                 forage_adj: Optional[float] = 1):
        self.animal = animal
        self.animal_choices = animal_choices
        self.id = id
        self.place_type = place_type
        self.name = name
        self.price = price
        self.upkeep_price = upkeep_price
        self.type = type
        self.unit_max = unit_max
        self.food_cap = food_cap
        self.food_default = food_default.upper()
        self.pallet_fill = pallet_fill
        self.pallet_maxno = pallet_maxno
        self.water_auto = water_auto
        self.animal_choices = animal_choices
        self.capacity = capacity
        self.current_food = [food_default.upper()]
        self.ledger = [{'month': 0, 'transaction': self.id, 'amount': round(-self.price, 2)}]
        self.animals = list()
        if fill is None:
            self.fill = dict()
        else:
            self.fill = fill
        self.available_fill_in = [{'fill_type': x['fill_type'], 'price_l': x['price_l']}
                                  for x in self.capacity if x['fill_type'] in ['STRAW', 'WATER'] and x['capacity'] > 0]
        self.available_fill_in.extend([{'fill_type': x['fill_type'], 'price_l': x['price_l']}
                                       for x in self.animal.group_fill])
        if not pd.isna(self.water_auto) and self.water_auto is not None:
            self.fill['WATER'] = float('inf')
            self.available_fill_in.append({'fill_type': 'WATER', 'price_l': 0})
        self.auto_buy = auto_buy
        self.auto_sell = auto_sell
        self.point_age = 0
        self.extension = []
        self.sell_price = 0
        self.forage_adj = forage_adj
        self.update_point_value()

        self.animal_autosell = animal_autosell
        self.profit = 0
        self.update_profit()

    def buy_manure_heap(self):
        for i in range(len(self.capacity)):
            if self.capacity[i]['fill_type'] == 'MANURE':
                index = i
            else:
                index = None
        if index:
            self.capacity[index]['capacity'] += 4000000.0
            self.ledger.append({'month': self.point_age, 'transaction': 'manureHeapExtension', 'amount': -25000.00})
            self.extension.append({'id': 'manureHeapExtension', 'price': 25000.00, 'upkeep_price': 25})
            self.update_point_value()

    def buy_fill(self, fill_type, amount):
        for ft in self.available_fill_in:
            if ft['fill_type'] == 'FORAGE':
                adj = self.forage_adj
            else:
                adj = 1
            if ft['fill_type'] == fill_type:
                if self.fill.get(fill_type):
                    self.fill[fill_type] += amount
                else:
                    self.fill[fill_type] = amount
                self.ledger.append({'month': self.point_age, 'transaction': fill_type,
                                    'amount': round(ft['price_l'] * -amount * adj, 2)})
                self.update_animal_fill()
        self.update_point_value()

    def buy_animals(self, breed: str, age: Optional[int] = 0, amount: Optional[int] = 1, free: Optional[bool] = False):
        animal_subtype = next(iter([x for x in self.animal_choices if x.subtype == breed]))
        if animal_subtype:
            # need to do this in order to standardize age purchase with available age options
            prices = pd.DataFrame(animal_subtype.price_buy).query(f"age_mo <= {age}")
            age_max = prices.nlargest(1, 'age_mo')
            age_std = age_max.iloc[0]['age_mo']
            new_animals = Animal(animal_subtype, age_mo=age_std, units=amount)
            cost = (new_animals.price_buy * amount) + (new_animals.price_trans * amount)
            if not free:
                self.ledger.append({'month': self.point_age, 'transaction': breed, 'amount': round(-cost, 2)})
            self.animals.append(new_animals)
            self.update_point_value()

    def update_animal_fill(self):
        allowed = [x['fill_type'] for x in self.available_fill_in]
        avail = [x[0] for x in self.fill.items() if x[1] > 0 and x[0] in allowed]
        for a in self.animals:
            a.available_inputs = avail
            a.update()

    def get_animal_fill(self):
        fill_in = dict()
        fill_out = dict()
        for a in self.animals:
            for f in a.fill_in:
                if f['fill_type'] == 'food':
                    if a.subtype.type.consumption == 'SERIAL':
                        fill_df = pd.DataFrame(self.current_food, columns=['fill_type']).\
                            assign(eat_wgt=1)
                    else:
                        fill_pre = pd.DataFrame(self.current_food, columns=['fill_type'])
                        grp_df = pd.DataFrame(self.animal.group_fill)
                        wgt_df = pd.DataFrame(self.animal.food_group)
                        join_df = grp_df.merge(wgt_df, on='title').\
                            merge(fill_pre, on='fill_type')
                        min_grp_df = join_df.iloc[join_df.groupby('title')['price_l'].idxmin().tolist()]
                        fill_df = min_grp_df[['fill_type', 'eat_wgt']]
                    for fill in fill_df.to_dict('records'):
                        fi = fill_in.get(fill['fill_type'])
                        if fi:
                            fill_in[fill['fill_type']] += math.ceil(f['liter_day'] * a.units * fill['eat_wgt'])
                        else:
                            fill_in[fill['fill_type']] = math.ceil(f['liter_day'] * a.units * fill['eat_wgt'])
                else:
                    fi = fill_in.get(f['fill_type'].upper())
                    if fi:
                        fill_in[f['fill_type'].upper()] += f['liter_day'] * a.units
                    else:
                        fill_in[f['fill_type'].upper()] = f['liter_day'] * a.units
        # buy fill in case the point doesn't have it
        if self.auto_buy:
            for ft, fa in fill_in.items():
                if ft == 'FORAGE':
                    adj = self.forage_adj
                else:
                    adj = 1
                if fa > 0:
                    price_list_in = [x['price_l'] for x in self.available_fill_in if x['fill_type'] == ft]
                    if price_list_in:
                        price = next(iter(price_list_in))
                        if price > 0:
                            self.ledger.append({'month': self.point_age, 'transaction': ft,
                                                'amount': round(-(price * fa * adj), 2)})
                            exists = self.fill.get(ft)
                            if exists:
                                self.fill[ft] += fa
                            else:
                                self.fill[ft] = fa

        # update animal fill before getting outputs
        self.update_animal_fill()

        # fill out
        for a in self.animals:
            for f in a.fill_out:
                if f['fill_type'] == 'pallets':
                    fo_type = self.pallet_fill
                else:
                    fo_type = f['fill_type'].upper()
                fo = fill_out.get(fo_type)
                if fo:
                    fill_out[fo_type] += f['liter_day'] * a.units * f['prod_wgt']
                else:
                    fill_out[fo_type] = f['liter_day'] * a.units * f['prod_wgt']

        # add outputs to storage or autosell
        for ft, fa in fill_out.items():
            if fa > 0:
                if self.auto_sell:
                    price_list_out = [x['price_l'] for x in self.capacity if x['fill_type'] == ft]
                    if price_list_out:
                        price = next(iter(price_list_out))
                        if price > 0:
                            self.ledger.append({'month': self.point_age, 'transaction': ft,
                                                'amount': round(price * fa, 2)})
                else:
                    exists = self.fill.get(ft)
                    if exists:
                        self.fill[ft] += fa
                    else:
                        self.fill[ft] = fa

        # clear the used inputs from the point storage
        for ft, fa in fill_in.items():
            if fa > 0:
                exists = self.fill.get(ft)
                if exists:
                    self.fill[ft] = max(self.fill[ft] - fa, 0)
                else:
                    self.fill[ft] = 0

    def increase_pt_age(self, months: Optional[int] = 1):
        if self.animal_autosell not in ['none', 'old', 'new', 'all', 'mature']:
            print("animal_autosell must be one of ['none', 'old', 'new', 'all', 'mature'].")
            print("animal_autosell being set to 'none'")
            self.animal_autosell = 'none'
        for i in range(months):
            self.get_animal_fill()
            for a in self.animals:
                a.increase_age()
            for a in self.animals:
                # second for loop needed as to not modify list in place and thus add age to new animals
                self.reproduce(a)
                if self.animal_autosell in ['mature']:
                    self.autosell(a)
            self.ledger.append({'month': self.point_age, 'transaction': '_'.join((self.id, 'upkeep')),
                                'amount': -self.upkeep_price})
            for e in self.extension:
                self.ledger.append({'month': self.point_age, 'transaction': '_'.join((e['id'], 'upkeep')),
                                    'amount': -e['upkeep_price']})
            self.point_age += 1
        self.update_point_value()
        self.update_profit()

    def reproduce(self, animal):
        current_units = 0
        for a in self.animals:
            current_units += a.units
        empty_slots = self.unit_max - current_units
        if animal.reprod_mo >= animal.subtype.reprod_duration_mo:
            if empty_slots > 0:
                new_amt = min(animal.units, empty_slots)
                remainder = animal.units - new_amt
                self.buy_animals(breed=animal.subtype.subtype, age=0, amount=new_amt, free=True)
            if self.animal_autosell in ['new', 'old', 'all']:
                self.autosell(animal, buy_more=remainder)
            # need to set this to 1 instead of 0, reprocution doesn't go down to 0 in game
            animal.reprod_mo = 1

    def sell_animals(self, animal, units):
        min_units = min(units, animal.units)
        sell_price = animal.price_sell - animal.price_trans
        self.ledger.append({'month': self.point_age, 'transaction': animal.subtype.subtype,
                            'amount': round(sell_price * min_units, 2)})
        animal.units -= min_units
        if animal.units <= 0:
            if animal.units < 0:
                print("WARNING: animal units somehow < 0!")
            self.animals.remove(animal)
        self.update_point_value()

    def update_point_value(self):
        sell_price = 0
        sell_price += self.price/2.0
        for e in self.extension:
            sell_price += e['price']/2.0

        # get fill prices
        fill_df = pd.DataFrame.from_dict(self.fill, orient='index', columns=['amount_l']).rename_axis('fill_type').\
            reset_index()
        fill_df = fill_df[fill_df['amount_l'] != np.inf]
        food_df = pd.DataFrame(self.animal.group_fill)[['fill_type', 'price_l']]
        cap_df = pd.DataFrame(self.capacity)[['fill_type', 'price_l']]
        total_df = pd.concat([cap_df, food_df], axis=0)
        price_amt_df = pd.merge(total_df, fill_df, on='fill_type', how='inner')
        price_amt_df['price_total'] = price_amt_df['price_l'] * price_amt_df['amount_l']
        sell_price += price_amt_df['price_total'].sum()
        for a in self.animals:
            animal_price = (a.price_sell - a.price_trans) * a.units
            sell_price += animal_price
        self.sell_price = sell_price

    def autosell(self, animal, buy_more=0):
        breed = animal.subtype.subtype
        units = animal.units
        if self.animal_autosell == 'all':
            for a in self.animals:
                self.sell_animals(animal=a, units=a.units)
            self.buy_animals(breed=breed, age=0, amount=units, free=False)
        elif self.animal_autosell == 'new':
            for a in self.animals:
                if a.age_mo == 0:
                    self.sell_animals(animal=a, units=a.units)
        elif self.animal_autosell == 'old':
            self.sell_animals(animal=animal, units=animal.units)
            if buy_more > 0:
                self.buy_animals(breed=breed, age=0, amount=buy_more, free=False)
        elif self.animal_autosell == 'mature':
            sell_df = pd.DataFrame(animal.subtype.price_sell)
            max_price = sell_df['price_unit'].max()
            max_df = sell_df[sell_df['price_unit'] == max_price]
            sell_age = max_df['age_mo'].min()
            if animal.age_mo >= sell_age:
                self.sell_animals(animal=animal, units=animal.units)

    def update_profit(self):
        ledger_df = pd.DataFrame(self.ledger)
        self.profit = round(ledger_df['amount'].sum(), 2)


class AnimalType:
    def __init__(self, type: str, consumption: str, food_group: list[dict], group_fill: list[dict]):
        self.type = type
        self.consumption = consumption
        self.food_group = food_group
        self.group_fill = group_fill


class Breed(AnimalType):
    def __init__(self, parent, subtype: str, reprod_agemin_mo: Optional[int] = None,
                 reprod_duration_mo: Optional[int] = None, reprod_healthmin: Optional[float] = None,
                 fill_in: Optional[list[dict]] = None, fill_out: Optional[list[dict]] = None,
                 price_buy: Optional[list[dict]] = None, price_sell: Optional[list[dict]] = None,
                 price_trans: Optional[list[dict]] = None):
        self.type = parent
        self.subtype = subtype
        self.reprod_agemin_mo = reprod_agemin_mo
        self.reprod_duration_mo = reprod_duration_mo
        self.reprod_healthmin = reprod_healthmin
        if fill_in is not None:
            self.fill_in = fill_in
        else:
            self.fill_in = []
        if fill_out is not None:
            self.fill_out = fill_out
        else:
            self.fill_out = []
        if price_buy is not None:
            self.price_buy = price_buy
        else:
            self.price_buy = []
        # both price trans and price sell need to be modeled out between values, as this is how the game does it
        if price_sell is not None:
            self.price_sell_base = price_sell
            self.price_sell = self.price_interpolate(prices = self.price_sell_base)
        else:
            self.price_sell = []
        if price_trans is not None:
            self.price_trans_base = price_trans
            self.price_trans = self.price_interpolate(prices = self.price_trans_base)
        else:
            self.price_trans = []
    
    @staticmethod
    def price_interpolate(prices):
        prices_interp = []
        if prices is not None:
            for i in range(0, len(prices)-1):
                prices_interp.append(prices[i])
                age_min = prices[i].get('age_mo')
                age_max = prices[i+1].get('age_mo')
                age_dif = age_max - age_min
                sell_min = prices[i].get('price_unit')
                sell_max = prices[i+1].get('price_unit')
                price_dif = sell_max - sell_min
                mo_increase = price_dif/age_dif
                for j in range(1, age_dif):
                    prices_interp.append({'age_mo': age_min + j, 
                                        'price_unit': round(sell_min + (j * mo_increase), 2)})
            prices_interp.append(prices[-1])
        return prices_interp
                



class Animal(Breed):
    def __init__(self, parent, age_mo: Optional[int] = 0, units: Optional[int] = 1,
                 available_inputs: Optional[list] = None, reprod_mo: Optional[int] = 0):
        self.subtype = parent
        self.age_mo = age_mo
        self.units = units
        self.reprod_mo = reprod_mo
        if available_inputs is None:
            self.available_inputs = []
        else:
            self.available_inputs = available_inputs
        self.food_prod_wgt = 0
        self.water_prod_wgt = 0
        self.manure_prod_wgt = 0
        self.health = 0

        # filled in from Breed
        self.fill_in = []
        self.fill_out = []
        self.price_buy = []
        self.price_sell = []
        self.price_trans = []

        self.update()

        # update fill amounts and prices, etc
    def update(self):
        self.update_fill_price()
        self.update_prod_wgt()

    def update_fill_price(self):
        fill_in_df = pd.DataFrame(self.subtype.fill_in).query(f"age_mo <= {self.age_mo}")
        fill_in_max = fill_in_df.groupby('fill_type').agg({'age_mo': 'max'})
        fill_in_join = pd.merge(fill_in_df, fill_in_max, on=['fill_type', 'age_mo'], how='inner')
        self.fill_in = fill_in_join[['fill_type', 'liter_day']].to_dict('records')

        fill_out_df = pd.DataFrame(self.subtype.fill_out).query(f"age_mo <= {self.age_mo}")
        fill_out_max = fill_out_df.groupby('fill_type').agg({'age_mo': 'max'})
        fill_out_join = pd.merge(fill_out_df, fill_out_max, on=['fill_type', 'age_mo'], how='inner')
        fill_out_join['prod_wgt'] = 0
        self.fill_out = fill_out_join[['fill_type', 'liter_day']].to_dict('records')

        price_buy_df = pd.DataFrame(self.subtype.price_buy).query(f"age_mo <= {self.age_mo}")
        price_buy_max = price_buy_df.nlargest(1, 'age_mo')
        self.price_buy = price_buy_max.iloc[0]['price_unit']

        price_sell_df = pd.DataFrame(self.subtype.price_sell).query(f"age_mo <= {self.age_mo}")
        price_sell_max = price_sell_df.nlargest(1, 'age_mo')
        self.price_sell = price_sell_max.iloc[0]['price_unit']

        price_trans_df = pd.DataFrame(self.subtype.price_trans).query(f"age_mo <= {self.age_mo}")
        price_trans_max = price_trans_df.nlargest(1, 'age_mo')
        self.price_trans = price_trans_max.iloc[0]['price_unit']

    def update_prod_wgt(self):
        group_fill_df = pd.DataFrame(self.subtype.type.group_fill)
        food_group_df = pd.DataFrame(self.subtype.type.food_group)
        df_avail = pd.DataFrame(self.available_inputs, columns=['fill_type'])

        food_join = pd.merge(group_fill_df, food_group_df, on=['title'], how='inner')
        food_actual = pd.merge(food_join, df_avail, on=['fill_type'], how='inner')
        if food_actual.shape[0] > 0:
            food_chosen = food_actual.groupby('title').agg({'prod_wgt': 'max'})
            if self.subtype.type.consumption == 'SERIAL':
                self.food_prod_wgt = food_chosen.nlargest(1, 'prod_wgt').iloc[0]['prod_wgt']
            else:
                self.food_prod_wgt = food_chosen['prod_wgt'].sum()
        else:
            self.food_prod_wgt = 0
        if 'water' in [x.get('fill_type') for x in self.fill_in]:
            if 'WATER' in self.available_inputs:
                self.water_prod_wgt = 1
            else:
                self.water_prod_wgt = 0
        else:
            self.water_prod_wgt = 1
        self.health = self.food_prod_wgt * self.water_prod_wgt

        # manure
        if 'straw' in [x.get('fill_type') for x in self.fill_in]:
            if 'STRAW' in self.available_inputs:
                self.manure_prod_wgt = 1
            else:
                self.manure_prod_wgt = 0
        else:
            self.manure_prod_wgt = 1

        # calc prod_wgt
        for f in self.fill_out:
            if f['fill_type'] != 'manure':
                f['prod_wgt'] = self.health
            else:
                f['prod_wgt'] = self.health * self.manure_prod_wgt

    def increase_age(self, months: Optional[int] = 1):
        for i in range(months):
            if self.subtype.reprod_agemin_mo is not None:
                if self.age_mo >= self.subtype.reprod_agemin_mo:
                    if self.health >= self.subtype.reprod_healthmin:
                        if self.reprod_mo < self.subtype.reprod_duration_mo:
                            self.reprod_mo += 1
            self.age_mo += 1
        self.update()


def forage_adj_calc(con, hay, silage, straw, mineral):
    # this value adjusts the price of forage (TMR) to be line with the price of the components
    forage_dict = {'hay': hay, 'silage': silage, 'straw': straw, 'mineral': mineral}
    forage_sql = """
    WITH mix_wgt (name, wgt) AS (VALUES
    ('DRYGRASS_WINDROW', {hay}),
    ('SILAGE', {silage}),
    ('STRAW', {straw}),
    ('MINERAL_FEED', {mineral})

    ), forage_price AS (
    SELECT *
    FROM fill
    WHERE name = 'FORAGE' OR name IN (SELECT fill_types FROM animal_food_recipe)

    ), price_wgt_calc AS (
    SELECT a.*, b.pct_min, b.pct_max, c.wgt, price_l * wgt price_wgt
    FROM forage_price a
    LEFT JOIN animal_food_recipe b ON a.name = b.fill_types
    LEFT JOIN mix_wgt c ON a.name = c.name
    WHERE a.name != 'FORAGE'

    ), price_calc AS ( 
    SELECT 'FORAGE' name, sum(price_wgt) / sum(wgt) price_l_adj
    FROM price_wgt_calc
    )

    SELECT a.*, b.price_l_adj, b.price_l_adj/a.price_l forage_adj
    FROM forage_price a
    INNER JOIN price_calc b 
    WHERE a.name = 'FORAGE';"""

    forage_fill_sql = forage_sql.format(**forage_dict)
    forage_df = pd.read_sql_query(forage_fill_sql, con)
    forage_adj = round(forage_df.to_dict('records')[0].get('forage_adj'), 2)
    return forage_adj


db_path = os.path.expanduser('~/Games/farming_simulator22/scrape.sqlite')
base_fig_dir = os.path.expanduser('~/Games/farming_simulator22/prod_figs')

con = sqlite.connect(db_path)
# min adj = 50% hay, 30% straw, 20% silage, 0% mineral feed
forage_low = forage_adj_calc(con, hay = 0.5, silage = 0.2, straw = 0.3, mineral = 0)
# autofeeder in big barn = 37.5% hay, 20% straw, 37.5% silage and 5% mineral feed
forage_high = forage_adj_calc(con, hay = 0.375, silage = 0.375, straw = 0.2, mineral = 0.05)
point_where = "WHERE id IN ('cowBarnBig', 'cowBarnBigVector', 'chickenBarnBig', 'sheepBarnBig', 'pigBarnBig')"
point_sql = f"SELECT * FROM animal_point {point_where};"
animal_point_rows = pd.read_sql_query(point_sql, con).to_dict('records')

ap_list = []
for p in animal_point_rows:
    animal_type = p.get('type')
    point_id = p.get('id')
    if point_id in ['cowBarnSmall', 'cowBarnMedium', 'cowBarnBig']:
        forage_adj = forage_low
    elif point_id == 'cowBarnBigVector':
        forage_adj = forage_high
    else:
        forage_adj = 1
    animal_food = next(iter((pd.read_sql_query("SELECT * FROM animal_food WHERE type = ?;", con,
                                    params=[animal_type]).to_dict('records'))))
    animal_list = []
    consumption = animal_food.get('consumption')
    animal_fg = pd.read_sql_query("SELECT * FROM animal_food_group WHERE type = ?;", con,
                                  params=[animal_type]).to_dict('records')
    food_fill_sql = '\n'.join((
        "SELECT a.*, b.price_l FROM animal_food_fill a LEFT JOIN fill b ON a.fill_type = b.name WHERE type = ?;",
    ))

    animal_ff = pd.read_sql_query(food_fill_sql, con,
                                  params=[animal_type]).to_dict('records')
    type_class = AnimalType(type=animal_type, consumption=consumption, food_group=animal_fg, group_fill=animal_ff)
    animals = pd.read_sql_query("SELECT * FROM animal WHERE type = ?;", con, params=[animal_type])
    for s in animals.to_dict('records'):
        subtype = s.get('subtype')
        reprod_agemin_mo = s.get('reprod_agemin_mo')
        reprod_duration_mo = s.get('reprod_duration_mo')
        reprod_healthmin = s.get('reprod_healthmin')
        fill_in = pd.read_sql_query(
            "SELECT fill_type, age_mo, liter_day FROM animal_fill WHERE direction = 'in' AND subtype = ?;",
            con, params=[subtype]).to_dict('records')
        fill_out = pd.read_sql_query(
            "SELECT fill_type, age_mo, liter_day FROM animal_fill WHERE direction = 'out' AND subtype = ?;",
            con, params=[subtype]).to_dict('records')
        price_buy = pd.read_sql_query(
            "SELECT age_mo, price_unit FROM animal_price WHERE price_type = 'buy' AND subtype = ?;",
            con, params=[subtype]).to_dict('records')
        price_sell = pd.read_sql_query(
            "SELECT age_mo, price_unit FROM animal_price WHERE price_type = 'sell' AND subtype = ?;",
            con, params=[subtype]).to_dict('records')
        price_trans = pd.read_sql_query(
            "SELECT age_mo, price_unit FROM animal_price WHERE price_type = 'transport' AND subtype = ?;",
            con, params=[subtype]).to_dict('records')
        breed_class = Breed(type_class, subtype=subtype, reprod_agemin_mo=reprod_agemin_mo,
                            reprod_duration_mo=reprod_duration_mo, reprod_healthmin=reprod_healthmin,
                            fill_in=fill_in, fill_out=fill_out, price_buy=price_buy, price_sell=price_sell,
                            price_trans=price_trans)
        animal_list.append(breed_class)
    capacity_sql = '\n'.join((
        "SELECT a.pallet_fill fill_type,",
        "       CASE WHEN a.pallet_fill = 'EGG' THEN a.pallet_maxno * 1400",
        "            WHEN a.pallet_fill = 'WOOL' THEN a.pallet_maxno * 1000",
        "	         ELSE NULL END capacity,",
        "       b.price_l",
        "  FROM animal_point a",
        "  LEFT JOIN fill b ON a.pallet_fill = b.name",
        " WHERE id = ? AND pallet_fill IS NOT NULL",
        " UNION",
        "SELECT a.fill_type, a.capacity, b.price_l ",
        "  FROM animal_capacity a ",
        "  LEFT JOIN fill b ON a.fill_type = b.name ",
        " WHERE point_id = ?;",
    ))
    capacity_list = pd.read_sql_query(capacity_sql, con, params=[point_id, point_id]).to_dict('records')
    animal_point = PointAnimal(id=point_id, place_type=p.get('place_type'), name=p.get('name'), price=p.get('price'),
                               upkeep_price=p.get('upkeep_price'), type=animal_type, unit_max=p.get('unit_max'),
                               food_cap=p.get('food_cap'), food_default=p.get('food_default'), animal=type_class,
                               animal_choices=animal_list, capacity=capacity_list, pallet_fill=p.get('pallet_fill'),
                               pallet_maxno=p.get('pallet_maxno'), water_auto=p.get('water_auto'), forage_adj = forage_adj)

    ap_list.append(animal_point)

def calc_recoup(ap_list, point_id, breed, animal_amt, buy_age, 
                stock_sell = 'none', food = None, buy_manure_heap = False, 
                auto_buy = True, auto_sell = True, ledger_months = 40,
                add_mo = 0):
    """
    stock_sell needs to be in ['none', 'all', 'new', 'old', 'mature']:
    none: stock will not be sold automatically
    all: all stock will be sold after reproduction
    new: newly born stock will be sold after reproduction
    old: newly born stock will be kept and mothers sold after reproduction
    mature: stock will be sold once they reach peak price for selling
    """
    q = copy.deepcopy(next(iter([x for x in ap_list if x.id == point_id])))
    if buy_manure_heap:
        q.buy_manure_heap()
    if food is None:
        food = q.current_food
    else:
        q.current_food = food
    food_str = ', '.join([x.lower() for x in q.current_food])
    q.buy_animals(breed=breed, age=buy_age, amount=animal_amt)
    q.auto_buy = auto_buy
    q.auto_sell = auto_sell
    q.animal_autosell = stock_sell
    for i in range(400):
        if q.profit < 0:
            if not q.animals:
                q.buy_animals(breed=breed, age=buy_age, amount=animal_amt)
            q.increase_pt_age(1)
    recoup_age = q.point_age
    for i in range(add_mo):
        if not q.animals:
            q.buy_animals(breed=breed, age=buy_age, amount=animal_amt)
        q.increase_pt_age(1)
    q_df = pd.DataFrame(q.ledger)
    q_mo = q_df.groupby('month').sum()
    q_sum = q_mo['amount'].cumsum()

    # linear model
    if stock_sell != 'none':
        q_filt = q_df.query("transaction == @breed and amount > 0")['month'].tolist()
        q_sum_filt = q_sum[q_filt]
        qy = q_sum_filt.to_numpy().reshape(len(q_sum_filt), 1)
        qx = q_sum_filt.index.to_numpy().reshape(len(q_sum_filt), 1)
    else:
        q_sum_filt = q_sum
        qy = q_sum_filt.to_numpy().reshape(len(q_sum_filt), 1)[-ledger_months:]
        qx = q_sum_filt.index.to_numpy().reshape(len(q_sum_filt), 1)[-ledger_months:]
    qx_pred = q_sum.index.to_numpy().reshape(len(q_sum), 1)
    q_model = LinearRegression().fit(qx,qy)
    q_pred = q_model.predict(qx_pred)
    b = q_model.intercept_[0]
    m = q_model.coef_[0][0]
    b_rnd = round(b, 2)
    m_rnd = round(m, 2)

    plt.clf()
    q_sum.plot(title=
                 f'{point_id}; Stocked {animal_amt} @ age {buy_age}; '
                 f'Breed: {breed};\n Stock sell: {stock_sell}; '
                 f'Food: {food_str};\n'
                 f'Break even time: {recoup_age} months. '
                 f'Profit/mo = ${m_rnd}', 
               color = "black",
               xlabel="Time (months)", ylabel="Total return ($)", 
               xticks=np.arange(0, q.point_age, 6))
    plt.plot(qx_pred, q_pred, color = 'yellow', linewidth = 1, 
             linestyle = 'dashed')

    return {'m': m, 'b': b}, q, plt

# milk production, forage, age 0
l1, m1, p1 = calc_recoup(ap_list = ap_list,
    point_id = 'cowBarnBig', breed = 'COW_SWISS_BROWN', animal_amt = 80, 
    buy_age = 0, stock_sell = 'none', food = ['FORAGE'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = 
    True, ledger_months = 40)
p1.show()
p1.savefig(os.path.join(base_fig_dir, 'cowBarnBig_swiss_forage_80_age0.png'), 
           bbox_inches = 'tight')
p1.show()

# milk production, forage, age 24
l1a, m1a, p1a = calc_recoup(ap_list = ap_list,
    point_id = 'cowBarnBig', breed = 'COW_SWISS_BROWN', animal_amt = 80, 
    buy_age = 24, stock_sell = 'none', food = ['FORAGE'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = 
    True, ledger_months = 40)
p1a.savefig(os.path.join(base_fig_dir, 'cowBarnBig_swiss_forage_80_age24.png'), 
           bbox_inches = 'tight')
p1a.show()

# milk production, grass, age 0
l2, m2, p2 = calc_recoup(ap_list = ap_list, 
    point_id = 'cowBarnBig', breed = 'COW_SWISS_BROWN', animal_amt = 80, 
    buy_age = 0, stock_sell = 'none', food = ['GRASS_WINDROW'], 
    buy_manure_heap = True, auto_sell = True,
                         auto_buy = True, auto_sell = True, 
    ledger_months = 40)
p2.xticks(rotation=90)
p2.savefig(os.path.join(base_fig_dir, 'cowBarnBig_swiss_grass_80_age0.png'), 
           bbox_inches = 'tight')
p2.show()

# milk production, hay, age 0
l3, m3, p3 = calc_recoup(ap_list = ap_list, 
    point_id = 'cowBarnBig', breed = 'COW_SWISS_BROWN', animal_amt = 80, 
    buy_age = 0, stock_sell = 'none', food = ['DRYGRASS_WINDROW'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = True, 
    ledger_months = 40)
p3.savefig(os.path.join(base_fig_dir, 'cowBarnBig_swiss_hay_80_age0.png'), 
           bbox_inches = 'tight')
# p3.xticks(rotation=90)
p3.show()

# milk production, forage, age 0, with autofeeder
l4, m4, p4 = calc_recoup(ap_list = ap_list, 
    point_id = 'cowBarnBigVector', breed = 'COW_SWISS_BROWN', animal_amt = 80, 
    buy_age = 0, stock_sell = 'none', food = ['FORAGE'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = True, 
    ledger_months = 40)
p4.xticks(rotation=90)
p4.savefig(os.path.join(base_fig_dir, 
           'cowBarnBigVector_swiss_forage_80_age0.png'), 
           bbox_inches = 'tight')
p4.show()

# beef production, hay, age 0, half stocked, with reproduction
l4a, m4a, p4a = calc_recoup(ap_list = ap_list,
    point_id = 'cowBarnBig', breed = 'COW_ANGUS', animal_amt = 40, 
    buy_age = 0, stock_sell = 'mature', food = ['DRYGRASS_WINDROW'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = 
    True, ledger_months = 112)
p4a.xticks(rotation=90)
p4a.savefig(os.path.join(base_fig_dir, 'cowBarnBig_angus_hay_40_age0.png'), 
           bbox_inches = 'tight')
p4a.show()

# beef production, hay, age 0, half stocked, with reproduction, sell old
l4b, m4b, p4b = calc_recoup(ap_list = ap_list,
    point_id = 'cowBarnBig', breed = 'COW_ANGUS', animal_amt = 40, 
    buy_age = 0, stock_sell = 'old', food = ['DRYGRASS_WINDROW'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = 
    True, ledger_months = 112)
p4b.xticks(rotation=90)
p4b.savefig(os.path.join(base_fig_dir, 'cowBarnBig_angus_hay_40_age0_sellOld.png'), 
           bbox_inches = 'tight')
p4b.show()

# beef production, hay, age 0, half stocked, with reproduction, sell old
l4c, m4c, p4c = calc_recoup(ap_list = ap_list,
    point_id = 'cowBarnBig', breed = 'COW_ANGUS', animal_amt = 40, 
    buy_age = 0, stock_sell = 'old', food = ['FORAGE'], 
    buy_manure_heap = True, auto_buy = True, auto_sell = 
    True, ledger_months = 112)
p4c.xticks(rotation=90)
p4c.savefig(os.path.join(base_fig_dir, 'cowBarnBig_angus_forage_40_age0_sellOld.png'), 
           bbox_inches = 'tight')
p4c.show()

# egg production, barley, age 0
l5, m5, p5 = calc_recoup(ap_list = ap_list, 
    point_id = 'chickenBarnBig', breed = 'CHICKEN', animal_amt = 180, 
    buy_age = 0, stock_sell = 'none', food = ['BARLEY'], 
    auto_buy = True, auto_sell = True, ledger_months = 20)
p5.xticks(rotation=90)
p5.savefig(os.path.join(base_fig_dir, 
           'chickenBarnBig_barley_180_age0.png'), 
           bbox_inches = 'tight')
p5.show()

# egg and chick production, barley, age 0, half stock, with reprod,
l5a, m5a, p5a = calc_recoup(ap_list = ap_list, 
    point_id = 'chickenBarnBig', breed = 'CHICKEN', animal_amt = 90, 
    buy_age = 0, stock_sell = 'old', food = ['BARLEY'], 
    auto_buy = True, auto_sell = True, ledger_months = 40, add_mo = 40)
p5a.xticks(rotation=90)
p5a.savefig(os.path.join(base_fig_dir, 
           'chickenBarnBig_barley_90_age0_sellOld.png'), 
           bbox_inches = 'tight')
p5a.show()

# chicken production, barley, age 0, full stock, no reprod,
l5b, m5b, p5b = calc_recoup(ap_list = ap_list, 
    point_id = 'chickenBarnBig', breed = 'CHICKEN', animal_amt = 180, 
    buy_age = 0, stock_sell = 'mature', food = ['BARLEY'], 
    auto_buy = True, auto_sell = True, ledger_months = 40, add_mo = 40)
p5b.xticks(rotation=90)
p5b.savefig(os.path.join(base_fig_dir, 
           'chickenBarnBig_barley_90_age0_sellMature.png'), 
           bbox_inches = 'tight')
p5b.show()

# egg production, barley, age 6
l6, m6, p6 = calc_recoup(ap_list = ap_list, 
    point_id = 'chickenBarnBig', breed = 'CHICKEN', animal_amt = 180, 
    buy_age = 6, stock_sell = 'none', food = ['BARLEY'], 
    auto_buy = True, auto_sell = True, ledger_months = 20)
p6.xticks(rotation=90)
p6.savefig(os.path.join(base_fig_dir, 
           'chickenBarnBig_barley_180_age6.png'), 
           bbox_inches = 'tight')
p6.show()

# wool production, grass, age 0
l7, m7, p7 = calc_recoup(ap_list = ap_list, 
    point_id = 'sheepBarnBig', breed = 'SHEEP_LANDRACE', animal_amt = 65, 
    buy_age = 0, stock_sell = 'none', food = ['GRASS_WINDROW'], 
    auto_buy = True, auto_sell = True, ledger_months = 20)
p7.xticks(rotation=90)
p7.savefig(os.path.join(base_fig_dir, 
           'sheepBarnBig_grass_65_age0.png'), 
           bbox_inches = 'tight')
p7.show()

# wool production, grass, age 36
l7a, m7a, p7a = calc_recoup(ap_list = ap_list, 
    point_id = 'sheepBarnBig', breed = 'SHEEP_LANDRACE', animal_amt = 65, 
    buy_age = 36, stock_sell = 'none', food = ['GRASS_WINDROW'], 
    auto_buy = True, auto_sell = True, ledger_months = 20)
p7a.xticks(rotation=90)
p7a.savefig(os.path.join(base_fig_dir, 
           'sheepBarnBig_grass_65_age36.png'), 
           bbox_inches = 'tight')
p7a.show()

# wool & mutton production, grass, age 0, half stocked with reprod
l7b, m7b, p7b = calc_recoup(ap_list = ap_list, 
    point_id = 'sheepBarnBig', breed = 'SHEEP_LANDRACE', animal_amt = 33, 
    buy_age = 0, stock_sell = 'mature', food = ['GRASS_WINDROW'], 
    auto_buy = True, auto_sell = True, ledger_months = 40, add_mo = 40)
p7b.xticks(rotation=90)
p7b.savefig(os.path.join(base_fig_dir, 
           'sheepBarnBig_grass_33_age0_sellMature.png'), 
           bbox_inches = 'tight')
p7b.show()

# pork production, cheapest mix, age 0, fully stocked, no reproduction
l8, m8, p8 = calc_recoup(ap_list = ap_list, 
    point_id = 'pigBarnBig', breed = 'PIG_LANDRACE', animal_amt = 270, 
    buy_age = 0, stock_sell = 'mature', 
    food = ['MAIZE', 'BARLEY', 'CANOLA', 'SUGARBEET'], 
    auto_buy = True, auto_sell = True, ledger_months = 80, add_mo = 80)
p8.xticks(rotation=90)
p8.savefig(os.path.join(base_fig_dir, 
           'pigBarnBig_270_age0.png'), 
           bbox_inches = 'tight')
p8.show()

# pork production, cheapest mix, age 0, half stocked, with reproduction
l9, m9, p9 = calc_recoup(ap_list = ap_list, 
    point_id = 'pigBarnBig', breed = 'PIG_LANDRACE', animal_amt = 135, 
    buy_age = 0, stock_sell = 'mature', 
    food = ['MAIZE', 'BARLEY', 'CANOLA', 'SUGARBEET'], 
    auto_buy = True, auto_sell = True, ledger_months = 80, add_mo = 80)
p9.xticks(rotation=90)
p9.savefig(os.path.join(base_fig_dir, 
           'pigBarnBig_135_age0.png'), 
           bbox_inches = 'tight')
p9.show()

# pork production, cheapest mix, age 0, quarter stocked, with reproduction
l9a, m9a, p9a = calc_recoup(ap_list = ap_list, 
    point_id = 'pigBarnBig', breed = 'PIG_LANDRACE', animal_amt = 67, 
    buy_age = 0, stock_sell = 'mature', 
    food = ['MAIZE', 'BARLEY', 'CANOLA', 'SUGARBEET'], 
    auto_buy = True, auto_sell = True, ledger_months = 80, add_mo = 80)
p9a.xticks(rotation=90)
p9a.savefig(os.path.join(base_fig_dir, 
           'pigBarnBig_67_age0.png'), 
           bbox_inches = 'tight')
p9a.show()
