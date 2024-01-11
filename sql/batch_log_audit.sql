select top 400 CAST(end_date_time AS DATE) as Date, left(file_name, 4) as 'Source system', source_system as Stream, batch_status, sum(number_of_rows) as 'Number of rows',
datediff(mi,min(start_date_time),max(end_date_time)) as 'duration, min',
sum(number_of_rows)/datediff(mi,min(start_date_time),max(end_date_time)) as 'throughput, lines/min', max(end_date_time) as End_Date_Time
from claim_batch_log
where source_system in ('HCFA','UB92')
and batch_status in ('terminal', 'scored', 'published','loaded')
and number_of_rows>0
group by CAST(end_date_time AS DATE), left(file_name, 4), source_system, batch_status
order by  MAX(end_date_time) desc
