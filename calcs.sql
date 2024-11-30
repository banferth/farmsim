WITH reprod AS (
SELECT type, min(reprod_agemin_mo) reprod_agemin_mo, min(reprod_duration_mo) reprod_duration_mo, 
       min(reprod_healthmin) reprod_healthmin
  FROM animal GROUP BY type

), mix AS (
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
SELECT a.name, a.title, a.show, a.unit,
	   CASE WHEN b.mass IS NULL THEN a.mass_l ELSE b.mass END mass_l,
       CASE WHEN b.price IS NULL THEN a.price_l ELSE b.price END price_l
  FROM fill a
  LEFT JOIN tmr b ON a.name = b.name
)

SELECT a.type, a.consumption, b.title, b.prod_wgt, b.eat_wgt, c.fill_type,
       d.reprod_agemin_mo, d.reprod_duration_mo, d.reprod_healthmin,
	   e.title, e.unit, e.mass_l, e.price_l
  FROM animal_food a
  LEFT JOIN animal_food_group b ON a.type = b.type
  LEFT JOIN animal_food_fill c ON b.type = c.type AND b.title = c.title
  LEFT JOIN reprod d ON a.type = d.type
  LEFT JOIN fill_alt e ON c.fill_type = e.name;
