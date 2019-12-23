forex_history = LOAD 'hdfs://hadoop-master:54310/user/hduser/market/EURUSD_GBP_CHF.csv' USING
   PigStorage(',') as (eurusd_date:chararray, eurusd_time:chararray, eurusd_open:float, eurusd_max:float, eurusd_min:float, eurusd_close:float, eurusd_volume:float, 
                       eurgbp_date:chararray, eurgbp_time:chararray, eurgbp_open:float, eurgbp_max:float, eurgbp_min:float, eurgbp_close:float, eurgbp_volume:float, 
					   eurchf_date:chararray, eurchf_time:chararray, eurchf_open:float, eurchf_max:float, eurchf_min:float, eurchf_close:float, eurchf_volume:float);

eurusd_data = FOREACH forex_history GENERATE ToDate(eurusd_date,'yyyy.MM.dd') as new_eurusd_date, eurusd_date, eurusd_time, eurusd_open, eurusd_max, eurusd_min, eurusd_close, eurusd_volume;
eurgbp_data = FOREACH forex_history GENERATE ToDate(eurgbp_date,'yyyy.MM.dd') as new_eurgbp_date, eurgbp_date, eurgbp_time, eurgbp_open, eurgbp_max, eurgbp_min, eurgbp_close, eurgbp_volume;

filtered_eurusd = FILTER eurusd_data BY (ToDate((chararray)$start_date, 'yyyyMMdd') < new_eurusd_date) AND (AddDuration(ToDate((chararray)$start_date, 'yyyyMMdd'), 'P1M') > new_eurusd_date) AND (eurusd_max == (float)$maxValue1);
filtered_eurgbp = FILTER eurgbp_data BY (ToDate((chararray)$start_date, 'yyyyMMdd') < new_eurgbp_date) AND (AddDuration(ToDate((chararray)$start_date, 'yyyyMMdd'), 'P1M') > new_eurgbp_date) AND (eurgbp_max == (float)$maxValue2);

group_eurusd = GROUP filtered_eurusd ALL;
group_eurgbp = GROUP filtered_eurgbp ALL;

count_eurusd = FOREACH group_eurusd GENERATE 'EURUSD', COUNT(filtered_eurusd);
count_eurgbp = FOREACH group_eurgbp GENERATE 'EURGBP', COUNT(filtered_eurgbp);

result = UNION count_eurusd, count_eurgbp;

STORE result INTO 'hdfs://hadoop-master:54310/user/hduser/results/oozieLab5Lytvyniuk/pig/mapred' USING PigStorage (',');
