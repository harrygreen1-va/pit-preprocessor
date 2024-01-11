-- alter table claim_batch_log drop column number_of_conformant_rows;

alter table claim_insurance_raw
    alter column insurance_company_name_a varchar(120);
alter table claim_insurance_raw
    alter column insurance_company_name_b varchar(120);
alter table claim_insurance_raw
    alter column insurance_company_name_c varchar(120);


