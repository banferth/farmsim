WITH pallet AS (
SELECT type, pallet_fill, 'pallets' fill_type
  FROM animal_point
 WHERE pallet_fill IS NOT NULL
 GROUP BY type, pallet_fill, fill_type

), out_group AS (
SELECT type, fill_type, age_mo, min(liter_day) liter_day 
  FROM animal_fill
 WHERE direction = 'out'
 GROUP BY type, fill_type, age_mo

), out_pallet AS (
SELECT a.type, a.fill_type, a.age_mo, a.liter_day, b.pallet_fill
  FROM out_group a
  LEFT JOIN pallet b ON a.type = b.type AND a.fill_type = b.fill_type
)

SELECT type, 
       CASE WHEN pallet_fill IS NULL THEN upper(fill_type) ELSE pallet_fill END fill_type,
	   age_mo, liter_day
  FROM out_pallet;
