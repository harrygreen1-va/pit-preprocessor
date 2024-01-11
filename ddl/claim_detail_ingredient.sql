create table [dbo].[CLAIM_DETAIL_INGREDIENT]
(

    [claim_detail_ingredient_key] [int] identity (1,1) not null,

    [claim_detail_key] [int] null,

    [Claim_key] [int] null,

    [compound_product_id_qualifier] [char](2) null,

    [compound_product_id] [varchar](20) null,

    [compound_ingredient_quantity] [int] null,

    [compound_ingredient_drug_cost] [money] null,

    [compound_basis_of_cost] [varchar](2) null,

    [compund_ingredient_modifier_code] [char](2) null,

    [is_current] [char](1) null,

    [start_date] [date] null,

    [end_date] [date] null,

    [source_system] [varchar](20) null,

    [source_entity] [varchar](50) null,

    [row_id] [varchar](20) null,

    [etl_batch_id] [varchar](20) null
)