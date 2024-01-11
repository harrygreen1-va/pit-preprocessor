
alter table dim_va_claim drop column claim_natural_key;
alter table f_professional_medical_claim_details drop column line_natural_key;
alter table f_institutional_medical_claim_details drop column line_natural_key;

alter table dim_va_claim add claim_natural_key varchar(60);
alter table f_professional_medical_claim_details add line_natural_key varchar(80);
alter table f_institutional_medical_claim_details add line_natural_key varchar(80);

-- indexes
if exists (select name from sysindexes where name = 'dvc_natural_key')
    drop index  dvc_natural_key on dim_va_claim
go
create nonclustered index dvc_natural_key on dim_va_claim (claim_natural_key)  
include (claim_key, claim_id, etl_batch_id);

if exists (select name from sysindexes where name = 'prof_line_natural_key')
    drop index  prof_line_natural_key on f_professional_medical_claim_details
go
create nonclustered index prof_line_natural_key on f_professional_medical_claim_details (line_natural_key)  
    include (claim_key, claim_detail_key);

if exists (select name from sysindexes where name = 'inst_line_natural_key')
    drop index  inst_line_natural_key on f_institutional_medical_claim_details
go
create nonclustered index inst_line_natural_key on f_institutional_medical_claim_details (line_natural_key)  
    include (claim_key, claim_detail_key);




