--
-- regular production profit
--

DROP VIEW IF EXISTS profit_hour; 
CREATE VIEW profit_hour AS
WITH fill_free AS (
SELECT name, title, show, unit, mass_l, 
       CASE WHEN name = 'WATER' THEN 0 ELSE price_l END price_l
  FROM fill

), place_fill AS (
SELECT id point_id, type point_type, 
       lower(fill_type) prod_id, fill_type,
	   'out' direction, liter_day/24 amount, capacity,
	   0 sell_direct
  FROM placeable
 WHERE liter_day IS NOT NULL

), prod_in AS (
SELECT a.*, b.price_l, a.amount * b.price_l price_in
  FROM prod_fill a 
  LEFT JOIN fill_free b ON a.fill_type = b.name 
 WHERE a.direction = 'in'

), prod_in_sum AS (
SELECT point_id, point_type, prod_id, sum(price_in) price_in_sum
  FROM prod_in
 GROUP BY point_id, point_type, prod_id

), prod_out AS (
SELECT a.*, b.price_l, a.amount * b.price_l price_out
  FROM prod_fill a 
  LEFT JOIN fill_free b ON a.fill_type = b.name 
 WHERE a.direction = 'out'

), prod_out_sum AS (
SELECT point_id, point_type, prod_id, sum(price_out) price_out_sum
  FROM prod_out
 GROUP BY point_id, point_type, prod_id

), prod_join AS (
SELECT a.*, c.name, b.price_out_sum, b.price_out_sum - a.price_in_sum profit_cycle,
       c.cycles_hour, c.cost_hour
  FROM prod_in_sum a
  LEFT JOIN prod_out_sum b ON a.point_id = b.point_id 
        AND a.point_type = b.point_type
        AND a.prod_id = b.prod_id
  LEFT JOIN production c ON a.point_id = c.point_id
        AND a.point_type = c.point_type
        AND a.prod_id = c.id

), point_join AS (
SELECT a.*, (a.profit_cycle * a.cycles_hour) - a.cost_hour profit_hour,
       b.price point_price, b.shared_throughput
  FROM prod_join a
  LEFT JOIN prod_point b ON a.point_id = b.id AND a.point_type = b.type AND a.point_type = b.type

), cost_recoup AS (
SELECT *, (point_price / profit_hour) / 24 cost_recoup_days 
  FROM point_join 

), place_recoup_fill AS (
SELECT a.id point_id, a.type point_type, lower(a.fill_type) prod_id, 
       0 price_in_sum, a.name, 
	   a.liter_day/24 * b.price_l price_out_sum,
	   NULL profit_cycle, NULL cycles_hour,
	   a.price_upkeep_day/24.0 cost_hour,
	   a.liter_day/24 * b.price_l profit_hour,
	   a.price point_price, 0 shared_throughput,
	   a.price / (a.liter_day * b.price_l) cost_recoup_days
  FROM placeable a
  LEFT JOIN fill b ON a.fill_type = b.name
 WHERE a.liter_day IS NOT NULL

), place_recoup_direct AS (
SELECT id point_id, type point_type,
       CASE WHEN category = 'generators' THEN lower('ELECTRICCHARGE') ELSE NULL END prod_id, 
       0 price_in_sum, name, 
	   income_hour price_out_sum,
	   NULL profit_cycle, NULL cycles_hour,
	   price_upkeep_day/24.0 cost_hour,
	   income_hour - (price_upkeep_day/24.0) profit_hour,
	   price point_price, 1 shared_throughput,
	   price / ((income_hour * 24) - price_upkeep_day) cost_recoup_days
  FROM placeable
 WHERE income_hour IS NOT NULL
)

SELECT * FROM cost_recoup UNION
SELECT * FROM place_recoup_fill UNION
SELECT * FROM place_recoup_direct
ORDER BY profit_hour DESC;

--
-- animal production profit  (not accurate for meat production)
--
DROP VIEW IF EXISTS animal_prod_profit_day;
CREATE VIEW animal_prod_profit_day AS
-- adjusts water to be free and the price of forage to be minimum costs 
-- based on input prices assuming a mixer
WITH mix AS (
SELECT name, title, show, unit, mass_l, price_l,
       CASE WHEN name = 'DRYGRASS_WINDROW' THEN 0.5 
	        WHEN name = 'STRAW' THEN 0.3
		    WHEN name = 'SILAGE' THEN 0.2 END pct_mix 
  FROM fill 
 WHERE name IN ('DRYGRASS_WINDROW', 'SILAGE', 'STRAW')
 
), wgts AS (
 SELECT name, mass_l * pct_mix mass_wgt, price_l * pct_mix price_wgt
   FROM mix

), tmr AS (
SELECT 'FORAGE' name, sum(mass_wgt) mass, sum(price_wgt) price FROM wgts

), fill_alt AS (
-- customizes forage price to reflect opportunity cost of selling ingredients
SELECT a.name, a.title, a.show, a.unit,
	   coalesce(b.mass, a.mass_l) mass_l,
       coalesce(b.price, a.price_l) price_l
  FROM fill a
  LEFT JOIN tmr b ON a.name = b.name

), fill_free AS (
-- assumes you are not paying water to fill animal points
SELECT name, title, show, unit, mass_l, 
       CASE WHEN name = 'WATER' THEN 0 
			ELSE price_l END price_l
  FROM fill_alt

), afill_max AS (
SELECT subtype, type, fill_type, fill_type_sub, direction, age_mo, liter_day,
       row_number() over(partition by type, subtype, fill_type, fill_type_sub, direction order by age_mo desc) rn
  FROM animal_fill

), afood AS (
SELECT *, 'food' AS greater_type FROM animal_food_fill

), afill_in AS (
SELECT a.subtype, a.type, b.fill_type, b.title, a.direction, a.age_mo, a.liter_day,
       a.fill_type fill_subtype
  FROM afill_max a
 INNER JOIN afood b ON a.fill_type = b.greater_type AND a.type = b.type
 WHERE a.direction = 'in' AND rn = 1

), aprod_in AS (
SELECT a.*, b.price_l,
       c.consumption, d.prod_wgt, d.eat_wgt,
	   row_number() over(partition by a.type, a.subtype, a.title order by b.price_l) rn
  FROM afill_in a 
  LEFT JOIN fill_free b ON a.fill_type = b.name
  LEFT JOIN animal_food c ON a.type = c.type
  LEFT JOIN animal_food_group d ON a.type = d.type AND a.title = d.title

), aprod_in_parallel AS (
SELECT *, liter_day * eat_wgt * price_l price_wgt
  FROM aprod_in 
 WHERE consumption = 'PARALLEL' AND rn = 1

), aprod_in_parallel_calc AS (
SELECT subtype, type, group_concat(fill_type, ', ') fill_type, NULL title,
       direction, age_mo, fill_subtype, consumption,
       sum(prod_wgt) prod_wgt, sum(price_wgt) price_day 
  FROM aprod_in_parallel
 GROUP BY subtype, type, direction, age_mo, fill_subtype, consumption

), aprod_in_serial AS (
SELECT subtype, type, fill_type, title, direction, age_mo, fill_subtype, consumption,
       prod_wgt, liter_day * price_l price_day  
  FROM aprod_in
 WHERE consumption = 'SERIAL'

), aprod_in_union AS (
SELECT * FROM aprod_in_serial UNION
SELECT * FROM aprod_in_parallel_calc

), aprod_in_notfood AS (
SELECT a.subtype, a.type, UPPER(a.fill_type) fill_type, NULL title, a.direction, a.age_mo, a.liter_day,
       a.fill_type fill_subtype, b.price_l, b.price_l * a.liter_day price_day
  FROM afill_max a
  LEFT JOIN fill_free b ON UPPER(a.fill_type) = b.name
 WHERE a.direction = 'in' AND a.fill_type != 'food' AND a.rn = 1

), aprod_in_notfood_sum AS (
SELECT subtype, type, sum(price_day) price_day_other
  FROM aprod_in_notfood
 GROUP BY subtype, type 

), aprod_in_final AS (
SELECT a.*, coalesce(b.price_day_other, 0) price_day_other,
       a.price_day + coalesce(b.price_day_other, 0) cost_day 
  FROM aprod_in_union a
 LEFT JOIN aprod_in_notfood_sum b ON a.subtype = b.subtype

), aprod_out AS (
SELECT a.subtype, a.type, a.direction, a.age_mo, a.liter_day, a.fill_type fill_cat, c.fill_type capacity_type,
       CASE WHEN a.fill_type_sub IS NULL THEN upper(a.fill_type) ELSE a.fill_type_sub END fill_type,
       b.id, b.unit_max, b.price, b.upkeep_price
  FROM afill_max a
 INNER JOIN animal_point b ON a.type = b.type
 LEFT JOIN animal_capacity c on b.id = c.point_id AND upper(a.fill_type) = c.fill_type
 WHERE a.direction = 'out' and rn = 1
   AND (a.fill_type = 'pallets' OR c.fill_type IS NOT NULL) 
 
), aprod_out_calc AS (
SELECT a.id, a.type, a.subtype, a.direction, a.age_mo, a.fill_type, a.unit_max, 
       a.price point_cost, a.upkeep_price point_upkeep, a.liter_day, b.price_l,
	   a.liter_day * b.price_l * unit_max price_day, fill_type = 'MANURE' has_manure
  FROM aprod_out a
  LEFT JOIN fill b ON a.fill_type = b.name

), aprod_out_sum AS (
SELECT id, type, subtype, direction, unit_max, point_cost, point_upkeep, 
       sum(price_day) revenue_day, max(has_manure) has_manure, max(age_mo) age_mo
  FROM aprod_out_calc
 GROUP BY id, type, subtype, direction, unit_max, point_cost, point_upkeep

), aprod_out_manure AS (
SELECT id, type, subtype, direction, age_mo, unit_max,
       CASE WHEN has_manure THEN point_cost + 25000 ELSE point_cost END point_cost,
	   CASE WHEN has_manure THEN point_upkeep + 25 ELSE point_upkeep END point_upkeep,
	   revenue_day, has_manure
  FROM aprod_out_sum

), buy_age AS (
SELECT subtype, max(age_mo) age_max FROM aprod_out GROUP BY subtype

), buy_price AS (
SELECT a.*, b.age_max, a.age_mo - b.age_max age_dif
  FROM animal_price a
  LEFT JOIN buy_age b ON a.subtype = b.subtype
  WHERE a.price_type in ('buy', 'transport')

), price_buy_rn AS (
SELECT subtype, type, price_type, age_mo, price_unit, age_max, age_dif,
       row_number() over(partition by subtype, price_type order by abs(age_dif) asc, age_dif desc) rn
  FROM buy_price

), price_buy_sum AS (
SELECT subtype, type, sum(price_unit) price_buy_unit
  FROM price_buy_rn
  WHERE rn = 1
  GROUP BY subtype, type

), aprod_out_final AS (
SELECT a.*, b.price_buy_unit, a.unit_max * b.price_buy_unit animal_cost
  FROM aprod_out_manure a
  LEFT JOIN price_buy_sum b ON a.subtype = b.subtype

), aprod_join AS (
SELECT a.id, a.type, a.subtype, a.unit_max, a.point_cost, a.point_upkeep, a.revenue_day, a.has_manure, 
       a.price_buy_unit, a.animal_cost,
       b.fill_type, b.consumption, b.prod_wgt, b.cost_day cost_day_unit, a.unit_max * b.cost_day cost_day,
	   a.revenue_day * b.prod_wgt revenue_day_adj
  FROM aprod_out_final a
  LEFT JOIN aprod_in_final b ON a.subtype = b.subtype

), aprod_calc AS (
SELECT id, type, subtype, unit_max, point_cost, point_upkeep, revenue_day, has_manure, 
       price_buy_unit, animal_cost, fill_type, consumption, prod_wgt, cost_day_unit, 
	   cost_day, revenue_day_adj, revenue_day_adj - (cost_day + point_upkeep) profit_day,
	   point_cost + animal_cost point_animal_cost
  FROM aprod_join
)

SELECT *, point_animal_cost / profit_day cost_recoup_days 
  FROM aprod_calc;


--
-- joined production with profit index calcs
--
DROP VIEW IF EXISTS prod_profit;
CREATE VIEW prod_profit AS
WITH prod_join AS (
SELECT point_id, point_type, prod_id, NULL food_choice, profit_hour * 24 profit_day, 
       shared_throughput, point_price upfront_cost, cost_recoup_days 
  FROM profit_hour
 UNION
SELECT id point_id, 'animal' point_type, subtype prod_id, fill_type food_choice, profit_day,
       NULL shared_throughput, point_animal_cost upfront_cost, cost_recoup_days
  FROM animal_profit_day 
)

SELECT *, profit_day/abs(cost_recoup_days) profit_index  
  FROM prod_join
 ORDER BY profit_day/abs(cost_recoup_days) DESC;


--
-- total point production, with shared vs not shared throughput taken into accout.
--
DROP VIEW IF EXISTS point_profit; 
CREATE VIEW point_profit AS
WITH max_profit AS (
SELECT point_id, point_type, upfront_cost, shared_throughput,
	   CASE WHEN shared_throughput = 0 THEN sum(profit_day)
	        WHEN shared_throughput = 1  OR shared_throughput IS NULL THEN max(profit_day) END profit_day_max
  FROM prod_profit
 GROUP BY point_id, point_type, upfront_cost, shared_throughput
)

SELECT * FROM max_profit ORDER BY profit_day_max DESC;

DROP VIEW IF EXISTS fruit_profit;
CREATE VIEW fruit_profit AS
WITH seeds AS (
SELECT name, 
       CASE WHEN name = 'rice' THEN 'RICESAPLINGS'
            WHEN name = 'poplar' THEN 'TREESAPLINGS'
	        WHEN name IN ('grass', 'grape', 'olive') THEN NULL
	        ELSE 'SEEDS' END seed_type,
       CASE WHEN name = 'grass' THEN 3
            WHEN name = 'spinach' THEN 2
            ELSE 1 END harvest_no
  FROM fruit

), fruit_join AS (
SELECT a.*, b.seed_type, b.harvest_no, c.price_l,
       a.seed_rate * coalesce(e.price_l, 0) seed_cost_m2,
	   a.liter_m2 * c.price_l * b.harvest_no fruit_revenue_m2,
	   a.windrow_liter_m2 * d.price_l * b.harvest_no windrow_revenue_m2
  FROM fruit a
  LEFT JOIN seeds b ON a.name = b.name
  LEFT JOIN fill c ON upper(a.name) = c.name
  LEFT JOIN fill d ON upper(a.windrow_out) = d.name
  LEFT JOIN fill e ON b.seed_type = e.name
)

SELECT *, coalesce(fruit_revenue_m2, 0) + coalesce(windrow_revenue_m2, 0) - coalesce(seed_cost_m2, 0) profit_m2
  FROM fruit_join order by profit_m2 DESC;
