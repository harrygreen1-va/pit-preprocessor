select top 100 cbl.file_name, cbl.etl_batch_id, cbl.to_score_indicator, cbl.batch_status, edits.* from  source_edits edits
                                                                                                              join claim_batch_log cbl on cbl.etl_batch_id=edits.etl_batch_id
  where cbl.etl_batch_id='CCNNC_U190411203357'
  order by edits.last_updated_date desc 


select top 600 cbl.file_name, cbl.etl_batch_id, cbl.to_score_indicator, cbl.batch_status, edits.* from  source_edits edits
join claim_batch_log cbl on cbl.etl_batch_id=edits.etl_batch_id
where cbl.etl_batch_id like'CC%' and cbl.batch_status!='in process'
and (claim_key is null or (claim_detail_key_in is null and claim_detail_key_pr is null))
order by edits.last_updated_date desc



select distinct cbl.etl_batch_id, cbl.file_name, cbl.last_updated_date
from  source_edits edits
join claim_batch_log cbl on cbl.etl_batch_id=edits.etl_batch_id
where cbl.etl_batch_id like'CC%' and cbl.batch_status!='in process'
and (claim_key is null or (claim_detail_key_in is null and claim_detail_key_pr is null))
order by cbl.last_updated_date desc


select * from claim_batch_log
where etl_batch_id='CCNNC_H190516084135'
--CCNNC_H190326120108
-- sp_rename 'source_edits.edit_id', 'source_edit_id', 'COLUMN';

select * from f_professional_medical_claim_details
where etl_batch_id='CCNNC_H190516113958'

select * from f_institutional_medical_claim_details
where etl_batch_id='CCNNC_U190411203357'

select * from dim_va_claim
where etl_batch_id='VACDB_U190501164314'

select * from dim_va_claim
where claim_id='301910800002450000'

/*
1 CCNNC_H190516113958 5/20/2019 11:52:33 AM TERMINAL CCNN CCNN-TerminalStatus-HCFA-CCNNC-20190516.txt HCFA 1,104  Unsorted 
 1104  Unsorted 
 49  Unsorted 
 
2 CCNNC_H190516111810 5/20/2019 11:30:39 AM TERMINAL CCNN CCNN-TerminalStatus-HCFA-CCNNC-20190516.txt HCFA 1,104  Unsorted 
 1104  Unsorted 
 47  Unsorted 
 
3 CCNNC_H190517111813 5/20/2019 11:30:39 AM TERMINAL CCNN CCNN-TerminalStatus-HCFA-CCNNC-20190517.txt HCFA 729  Unsorted 
 729  Unsorted 
 14  Unsorted 
 
4 CCNNC_H190518111815 5/20/2019 11:30:39 AM TERMINAL CCNN CCNN-TerminalStatus-HCFA-CCNNC-20190518.txt HCFA 761  Unsorted 
 761  Unsorted 
 7  Unsorted 
 




/*
select claim_key, is_current, etl_batch_id from dim_va_claim
where claim_id='301905800000247000'
*/