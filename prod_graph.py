import math
import os
import sqlite3 as sqlite
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

db_path = os.path.expanduser('~/Games/farming_simulator22/scrape.sqlite')
con = sqlite.connect(db_path)

g = nx.DiGraph()

# fruit
fruit = pd.read_sql_query("SELECT * FROM fruit;", con).to_dict('records')
g.add_node("land")
for fr in fruit:
    d = dict(fr)
    name = d.pop('name')
    liter_m2 = d.pop('liter_m2')
    windrow_out = d.pop('windrow_out')
    windrow_liter_m2 = d.pop('windrow_liter_m2')
    g.add_node(name)
    g.add_edge('land', name, seed_rate = d.pop('seed_rate'))
    for k,v in d.items():
        g.add_nodes_from([(name, {k: v})])
    g.add_node(name.upper())
    g.add_edge(name, name.upper(), liter_m2 = liter_m2)
    if isinstance(windrow_out, str):
        g.add_node(windrow_out.upper())
        g.add_edge(name, windrow_out.upper(), windrow_liter_m2 = windrow_liter_m2)
# fill
fill = pd.read_sql_query("SELECT * FROM fill;", con).to_dict('records')
for f in fill:
    d = dict(f)
    name = d.pop('name')
    g.add_node(name)
    for k,v in d.items():
        g.add_nodes_from([(name, {k: v})])

# fruit/fill convert
type_convert = pd.read_sql_query("SELECT * FROM type_convert;", con).to_dict('records')
for t in type_convert:
    d = dict(t)
    name = d.pop('name')
    g.add_node(name)
    input = d.pop('input')
    output = d.pop('output')
    g.add_edge(input, name)
    g.add_edge(name, output, factor = d['factor'], windrow_factor = d['windrow_factor'])


# prod
prod_point = pd.read_sql_query("SELECT * FROM prod_point;", con).to_dict('records')
for p in prod_point:
    d = dict(p)
    id = d.pop('id')
    g.add_node(id)
    for k,v in d.items():
        g.add_nodes_from([(id, {k: v})])

prod_fill = pd.read_sql_query("SELECT * FROM prod_fill;", con).to_dict('records')
for p in prod_fill:
    d = dict(p)
    prod_id = d.pop('prod_id')
    point_id = d.pop('point_id')
    fill_type = d.pop('fill_type')
    direction = d.pop('direction')
    g.add_node(prod_id)
    g.add_node(fill_type)
    if direction == 'in':
        g.add_edge(prod_id, point_id)
        g.add_edge(fill_type, prod_id, amount = d['amount'], capacity = d['capacity'])
    if direction == 'out':
        g.add_edge(point_id, prod_id)
        g.add_edge(prod_id, fill_type, amount = d['amount'], capacity = d['capacity'],
                   sell_direct = d['sell_direct'])


# animal_point
animal_point = pd.read_sql_query("SELECT * FROM animal_point;", con).to_dict('records')
for a in animal_point:
    d = dict(a)
    id = d.pop('id')
    g.add_node(id)
    for k,v in d.items():
        g.add_nodes_from([(id, {k: v})])

animal_food = pd.read_sql_query("SELECT * FROM animal_food;", con).to_dict('records')
for a in animal_food:
    d = dict(a)
    g.add_node(d['type'], consumption = d['consumption'])

# animal
animal = pd.read_sql_query("SELECT * FROM animal;", con).to_dict('records')
for a in animal:
    d = dict(a)
    type = d.pop('type')
    subtype = d.pop('subtype')
    g.add_node(type)
    g.add_node(subtype)
    for k,v in d.items():
        g.add_nodes_from([(type, {k: v})])
    g.add_edge(subtype, type)

#  afg = pd.read_sql_query("SELECT * FROM animal_food_group;", con).to_dict('records')
#  for a in afg:
#      d = dict(a)
#      food = '_'.join((d['type'], 'food'))
#      g.add_node(food)
#      g.add_edge(food, d['type'])
#      g.add_node(d['title'])
#      g.add_edge(d['title'], food, prod_wgt = d['prod_wgt'], eat_wgt = d['eat_wgt'])

#  aff = pd.read_sql_query("SELECT * FROM animal_food_fill;", con).to_dict('records')
#  for a in aff:
#      d = dict(a)
#      food = '_'.join((d['type'], 'food'))
#      g.add_node(food)
#      g.add_node(d['fill_type'])
#      g.add_edge(d['fill_type'], food, title = d['title'])





nx.draw(g, with_labels=True)
plt.show()
