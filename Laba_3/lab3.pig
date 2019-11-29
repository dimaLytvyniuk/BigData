forex_history = LOAD 'hdfs://namenode:9000/user/root/input/dataset_example.csv' USING
   PigStorage(',') as (eurusd_date:chararray, eurusd_time:chararray, eurusd_open:float, eurusd_max:float, eurusd_min:float, eurusd_close:float, eurusd_volume:float, 
                       eurgbp_date:chararray, eurgbp_time:chararray, eurgbp_open:float, eurgbp_max:float, eurgbp_min:float, eurgbp_close:float, eurgbp_volume:float, 
					   eurchf_date:chararray, eurchf_time:chararray, eurchf_open:float, eurchf_max:float, eurchf_min:float, eurchf_close:float, eurchf_volume:float);

eurusd_data = FOREACH forex_history GENERATE ToDate(eurusd_date,'yyyy.MM.dd') as new_eurusd_date, eurusd_date, eurusd_time, eurusd_open, eurusd_max, eurusd_min, eurusd_close, eurusd_volume;

filtered = FILTER eurusd_data BY (ToDate((chararray)$start_date, 'yyyyMMdd') < new_eurusd_date) AND (AddDuration(ToDate((chararray)$start_date, 'yyyyMMdd'), 'P1M') > new_eurusd_date);

STORE filtered INTO 'hdfs://namenode:9000/user/root/output/marketResult21' USING PigStorage (',');
