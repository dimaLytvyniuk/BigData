SELECT SUM(CASE WHEN eurusd_date LIKE CONCAT('${hivevar:MONTH_USD}','.%') AND  eurusd_max=${hivevar:USD_MAX} THEN 1 ELSE 0 END) as eurusd_count, 
    SUM(CASE WHEN eurgbp_date LIKE CONCAT('${hivevar:MONTH_GBP}','.%') AND  eurgbp_max=${hivevar:GBP_MAX} THEN 1 ELSE 0 END) as eurgbp_count 
    FROM market.forex;
