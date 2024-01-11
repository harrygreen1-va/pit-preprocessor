select top 400 CAST(end_date_time AS DATE) as Date, left(file_name, 4) as 'Source system', source_system as Stream, batch_status, sum(number_of_rows) as 'Number of rows',
	datediff(mi,min(start_date_time),max(end_date_time)) as 'duration, min',
	sum(number_of_rows)/datediff(mi,min(start_date_time),max(end_date_time)) as 'throughput, lines/min', max(end_date_time) as End_Date_Time
from claim_batch_log
where source_system in ('HCFA','UB92')
and batch_status in ('terminal', 'scored', 'published','loaded')
and number_of_rows>0
group by CAST(end_date_time AS DATE), left(file_name, 4), source_system, batch_status
order by  MAX(end_date_time) desc


select top 60 etl_batch_id, left(file_name, 4) as 'Source system',  source_system as Stream, file_name, batch_status, end_date_time, number_of_rows, number_of_conformant_rows,
	datediff(mi,start_date_time,end_date_time) as 'duration, min',
	number_of_conformant_rows/datediff(mi,start_date_time,end_date_time) as 'throughput, lines/min'
from claim_batch_log
where 
--source_system in ('HCFA','UB92')
--batch_status in ('terminal', 'scored', 'published','loaded')
number_of_rows>0
--and file_name like 'ccn%'
and start_date_time>'2020-09-05'
-- preprod
--and file_name in ('cpe-terminal-details-202008172306.txt', 'CCNN-ClaimsToScore-UB04-CCNNC-20200811.txt', 'CCNN-ClaimsToScore-HCFA-CCNNC-20200811.txt', 'VACS-ClaimsToScore-HCFA-VACDB-20200807.txt')
--and file_name in ('CCNN-ClaimsToScore-HCFA-CCNNC-20200811.txt', 'CCNN-ClaimsToScore-UB04-CCNNC-20200811.txt', 'cpe-terminal-details-202008112304.txt', 'CCNN-TerminalStatus-HCFA-CCNNC-20200830.txt', 'CCNN-TerminalStatus-UB04-CCNNC-20200828.txt')
--group by CAST(end_date_time AS DATE), left(file_name, 4), source_system, batch_status
--order by  file_name, end_date_time desc
order by  end_date_time desc




select top 30 * from claim_batch_log
where file_name like 'ccnn%'
--and file_name = 'CCNN-ClaimsToScore-HCFA-CCNNC-20200811.txt'
--and batch_status != 'in process'
order by last_updated_date desc

