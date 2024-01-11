/*
2023-11-23 13:23:01.926 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064227000 Line count: 4
2023-11-23 13:23:01.930 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064361000 Line count: 3
2023-11-23 13:23:01.931 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064204000 Line count: 3
2023-11-23 13:23:01.932 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100080021000 Line count: 2
2023-11-23 13:23:01.932 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064365000 Line count: 1
2023-11-23 13:23:01.932 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302304500096014000 Line count: 3
2023-11-23 13:23:01.933 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064439000 Line count: 1
2023-11-23 13:23:01.933 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302304500096113000 Line count: 1
2023-11-23 13:23:01.934 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100079996000 Line count: 1
2023-11-23 13:23:01.934 INFO  p.e.f.f.FbcsFileProcessor   :223 - Batch: CCNNC_D230222132301 claim_id: CCNNC302305100064289000 Line count: 7
*/

select * from F_dental_claim_DETAILS line
join dim_va_claim claim on line.claim_key=claim.claim_key
where claim_id='302305100064289000'
and line.etl_batch_id='CCNNC_D230222132301'