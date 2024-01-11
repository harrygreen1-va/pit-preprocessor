
select line.etl_batch_id, count(claim_detail_key) as ingested_count_prof
from 
f_professional_medical_claim_details line
where 
line.etl_batch_id  in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%' )
group by line.etl_batch_id
order by etl_batch_id

select line.etl_batch_id, count(claim_detail_key) as ingested_count_inst
from 
f_institutional_medical_claim_details line
where 
line.etl_batch_id  in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%' )
group by line.etl_batch_id


select line.etl_batch_id, count(claim_detail_key) as ingested_count_dent
from
f_dental_claim_details line
where
line.etl_batch_id  in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_D%' )
group by line.etl_batch_id

select cbl.etl_batch_id, cbl.number_of_conformant_rows from claim_batch_log cbl where etl_batch_id in (select etl_batch_id from batch_id_to_test) order by etl_batch_id


-- Validate all ingested lines and claims
select count(*) from
etl.claim_line_checksum cls
where
etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%' )
and
not exists
(
select line.claim_detail_key
from F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS line
join dim_va_claim claim on claim.claim_key=line.claim_key
where
cls.claim_line_id=line.source_claim_line_id and cls.etl_batch_id=line.etl_batch_id
and claim.claim_id=cls.claim_id and line.file_row_num=cls.file_row_num)

select count(*) from
etl.claim_line_checksum cls
where
etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%' )
and
not exists
(
select line.claim_detail_key
from F_INSTITUTIONAL_MEDICAL_CLAIM_DETAILS line
join dim_va_claim claim on claim.claim_key=line.claim_key
where
cls.claim_line_id=line.source_claim_line_id and cls.etl_batch_id=line.etl_batch_id
and claim.claim_id=cls.claim_id and line.file_row_num=cls.file_row_num)


select count(*) from
etl.claim_line_checksum cls
where
etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_D%' )
and
not exists
(
select line.claim_detail_key
from F_DENTAL_CLAIM_DETAILS line
join dim_va_claim claim on claim.claim_key=line.claim_key
where
cls.claim_line_id=line.line_item_control_number and cls.etl_batch_id=line.etl_batch_id
and claim.claim_id=cls.claim_id and line.file_row_num=cls.file_row_num)