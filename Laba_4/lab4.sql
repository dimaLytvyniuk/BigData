SELECT * FROM (
SELECT 'EURUSD' as name1, COUNT(*) as count1 FROM market.forex where eurusd_date LIKE '2000.06.%' AND  eurusd_max=0.9626)
UNION ALL(
SELECT 'EURGBP' as name2, COUNT(*) as count2 FROM market.forex where eurgbp_date LIKE '1999.07.%' AND  eurgbp_max=0.6488);


SELECT * FROM (
SELECT COUNT(*)  FROM market.forex where eurusd_date LIKE '2000.06.%' AND  eurusd_max=0.9626
UNION ALL
SELECT COUNT(*) FROM market.forex where eurgbp_date LIKE '1999.07.%' AND  eurgbp_max=0.6488
) lab4;

SELECT SUM(CASE WHEN eurusd_date LIKE '2000.06.%' AND  eurusd_max=0.9626 THEN 1 ELSE 0 END) FROM market.forex
