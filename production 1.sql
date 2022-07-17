final as (
    
    select
        dc.stock_number,
        sold_state,
        retail_country,
        ft.first_retail_ready,
        -- b2b_deal_datetime,
        -- brand,
        -- car_mileage,
        -- built_year,
        handover_date,
        case
            when maximum_budget is not null and retail_country='SE' and first_retail_ready>='01-01-2021' then maximum_budget
            when maximum_budget is not null and retail_country='DE' then maximum_budget
            when total_cost_budget is not null and maximum_budget is not null and total_cost_budget>=maximum_budget then total_cost_budget
            when total_cost_budget is not null and maximum_budget is not null and total_cost_budget<maximum_budget then maximum_budget
            when total_cost_budget is null then maximum_budget
            else total_cost_budget end as max_tot_cost_max_budg,
        -- nvl(max_budget_per_car_total,max_budget_per_car_budg) as tot_budget,
        --damage_part,
        --damage_type,
        -- SUM(no_repair_needed) as no_repair_needed,
        -- SUM(repair_needed) as repair_needed,
        -- SUM(bo_source_tot) as bo_source,
        -- SUM(adm_source_tot) as adm_source,
        SUM(bo_source_nrn) as entry_check_imperfections,
        SUM(adm_source_nrn) as admin_imperfections,
        
        
        -- SUM(selected_bo) as shwon_to_pub,
        -- SUM(not_selected_bo) as not_shwon_to_pub,
        -- SUM(selected_bo_nrn) as shwon_to_pub_nrn,
        -- SUM(not_selected_bo_nrn) as not_shwon_to_pub_nrn,
        
        -- SUM(selected_app) as shown_to_cust,
        -- SUM(not_selected_app) as not_shown_to_cust,
        -- SUM(selected_app_nrn) as shown_to_cust_nrn,
        -- SUM(not_selected_app_nrn) as not_shown_to_cust_nrn,
        SUM(bo_source_rn) as entry_check_damages,
        SUM(adm_source_rn) as admin_damages
        
        
        
    
    from damage_complete as dc
    left join max_budget as mb on mb.stock_number=dc.stock_number
    left join first_tour as ft on dc.stock_number=ft.stock_number
    where 
    1
    and (service_id=last_service_id or service_id is null)
    and handover_date >= current_date - interval '60 days' 
    --and booking_month = '3'
    GROUP BY 1,2,3,4,5,6)
,

/*num_cars_ec as (
    select
    date_part(year, td.refurb_auth_date) as year,
    date_part(month, td.refurb_auth_date) as month,
    ra.retail_country,
    count(distinct ra.stock_number) as num_cars_entry_check,
    avg(entry_check_damages) as avg_damages_ec,
    avg(entry_check_imperfections) as avg_imperfections_ec
    
    -- entry_check_damages, entry_check_imperfections
    
    from wkda_dm_retail_logistics.retail_ad as ra
    left join first_ec as fe on ra.stock_number = fe.stock_number
    left join transition_dates as td on td.refurbishment_id=fe.refurbishment_id
    left join damage_reduced as dr on ra.stock_number=dr.stock_number
    
    where 1
    and ra.retail_country='DE'
    and year=2021
    AND ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    group by 1,2,3
)*/







-- select 

-- stock_number,
-- sold_state,
-- retail_country,
-- handover_date,

-- nvl(entry_check_imperfections, 0) + nvl(admin_imperfections,0) as total_imperfections,
-- nvl(entry_check_damages, 0) + nvl(admin_damages, 0) as total_damages,
-- max_tot_cost_max_budg

-- from final
-- join num_cars_ec on blabla...


-- select ra.stock_number, td.refurb_auth_date, dr.entry_check_damages

-- from wkda_dm_retail_logistics.retail_ad as ra
-- left join first_ec as fe on ra.stock_number = fe.stock_number
-- left join transition_dates as td on td.refurbishment_id=fe.refurbishment_id
-- left join damage_reduced as dr on ra.stock_number=dr.stock_number
--------------------------------------------------------------------------------------------
damage_complete as (
    select
        distinct ra.stock_number,
        rs.id as service_id,
        date(rs.created_on) as service_date,
        ra.state as sold_state,
        r.state as refurb_state,
        rad.type as damage_type,
        rad.part as damage_part,
        rad.id as damage_id,
        service_name,
        ra.retail_country,
        --rs.created_on as serv_creation,
        last_value(rs.id) over (partition by rad.id order by rs.created_on ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_service_id,
        date(nvl(cs.b2b_deal_datetime,ra.first_import_on)) as b2b_deal_datetime,
        
        case when service_name = 'NO_REPAIR_NEEDED' and rad.id is not null then 1 else null end as no_repair_needed,
        case when service_name<>'NO_REPAIR_NEEDED' and rs.state = 'PERFORMED' and rad.id is not null then 1 else null end as repair_needed,
        case when rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') then 1 else null end as bo_source_tot,
        case when rad.source_type in ('IMPORT') and rad.id is not null then 1 else null end as adm_source_tot,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.display_to_customer_frontend = 'true' and rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') then 1 else null end as bo_source_nrn,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.display_to_customer_frontend = 'true' and rad.source_type in ('IMPORT') then 1 else null end as adm_source_nrn ,
        case 
            when service_name = 'NO_REPAIR_NEEDED' and rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') then 'NEW'
            when service_name = 'NO_REPAIR_NEEDED' and rad.source_type in ('IMPORT') then 'OLD'
            else null end damage_source_nrn,
        case 
            when rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') then 'NEW'
            when rad.source_type in ('IMPORT') then 'OLD'
            else null end damage_source,
        case
            when r.state in ('OPEN','REFURBISHMENT_ORDERED','PREPARATION_STARTED','REFURBISHMENT_FEEDBACK_RECEIVED') then 'before_auth'
            when r.state in ('REFURBISHMENT_AUTHORIZED','REFURBISHMENT_STARTED','REFURBISHMENT_COMPLETED','QUALITY_CHECK_ORDERED','QUALITY_CHECK_COMPLETED','COMPLETED') then 'after_auth'
            when r.state in ('CANCELLED') then 'cancelled'
            else 'other' end as before_after_ref_auth,
        case when rad.customer_display = 'true' then 1 ELSE NULL END AS selected_bo,
        case when rad.customer_display = 'false' then 1 ELSE NULL END AS not_selected_bo,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.customer_display = 'true' then 1 ELSE NULL END AS selected_bo_nrn,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.customer_display = 'false' then 1 ELSE NULL END AS not_selected_bo_nrn,
        case when rad.customer_display is null then 1 ELSE NULL END AS empty_bo,
        case when rad.display_to_customer_frontend = 'true' then 1 ELSE NULL END AS selected_app,
        case when rad.display_to_customer_frontend = 'false' then 1 ELSE NULL END AS not_selected_app,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.display_to_customer_frontend = 'true' then 1 ELSE NULL END AS selected_app_nrn,
        case when service_name = 'NO_REPAIR_NEEDED' and rad.display_to_customer_frontend = 'false' then 1 ELSE NULL END AS not_selected_app_nrn,
        case when rad.display_to_customer_frontend is null then 1 ELSE NULL END AS empty_app,
        case 
            when service_name = 'NO_REPAIR_NEEDED' and part in ('Back left brake', 'Front right brake', 'Front left brake', 'Back right brake') then 'YES' 
            when service_name is null and part in ('Back left brake', 'Front right brake', 'Front left brake', 'Back right brake') then 'YES' 
            else null end as brake_damage,
        case when service_name <>'NO_REPAIR_NEEDED' and rs.state = 'PERFORMED' and service_name is not null and rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') then 1 else null end as bo_source_rn,
       case when service_name <>'NO_REPAIR_NEEDED' and rs.state = 'PERFORMED' and service_name is not null and rad.source_type in ('IMPORT') then 1 else null end as adm_source_rn ,
        json_extract_path_text(ca.vehicle,'modelApi','make',true) as brand,
        cl.mileage as car_mileage,
        cl.built_year,
        date(ro.car_handover_on) as handover_date
        
        
    FROM wkda_dm_retail_logistics.retail_ad AS ra
    left join wkda_retail_ad.retail_ad_damage as rad on rad.retail_ad_id = ra.id
    left join wkda_retail_refurbishment.refurbishment_service as rs on rs.retail_ad_damage_id = rad.id
    left join wkda_dm_retail_logistics.car_leads as cl on cl.stock_number = ra.stock_number
    left join wkda_dm_retail_logistics.car_sales as cs on cs.id = cl.id
    left join wkda_dm_retail_logistics.car_details as cd on cd.id = cl.id
    left join wkda_dm_retail_logistics.refurbishment as r on r.retail_ad_id=ra.id
    left join wkda_dm_retail_logistics.refurbishment as r2 on r2.retail_ad_id=ra.id and r2.created_on>r.created_on
    left join wkda_dm_retail_logistics.ad AS ca ON ca.id=ra.ad_id
    left join wkda_dm_retail_logistics.retail_order as ro on ra.stock_number=ro.stock_number
    left join wkda_dm_retail_logistics.retail_order AS ro2 ON ra.stock_number=ro2.stock_number and ro2.created > ro.created
    
    
    where
    1
    --and rad.id is not null
    and ra.is_test = 'false'
    --and ra.retail_country='DE'
   -- and date(nvl(cs.b2b_deal_datetime,ra.first_import_on))>= '01-01-2020'
    --and r.state in ('REFURBISHMENT_AUTHORIZED','REFURBISHMENT_STARTED','REFURBISHMENT_COMPLETED','QUALITY_CHECK_ORDERED','QUALITY_CHECK_COMPLETED','COMPLETED')
    and r2.id is null
    and brake_damage is null
    and ro2.id is null
   -- group by ra.stock_number,service_id,rs.created_on,ra.state,r.state,rad.type,rad.part,rad.id,rs.service_name,ra.retail_country,cs.b2b_deal_datetime,ra.first_import_on,rs.state
   ),
    

    
max_budget as(
    select
        ra.stock_number,
        SUM(
            case
                WHEN r.country = 'SE' THEN ((r.total_cost_budget_minor_units/100)/10.3)
                WHEN r.country = 'PL' and r.currency_code='PLN' THEN ((r.total_cost_budget_minor_units/100)/4.4)
                WHEN r.country = 'PL' and r.currency_code<>'PLN' THEN (r.total_cost_budget_minor_units/100)
                else (r.total_cost_budget_minor_units/100) END) as total_cost_budget,
        SUM(
            case
                WHEN r.country = 'SE' THEN ((r.maximum_budget_minor_units/100)/10.3)
                WHEN r.country = 'PL' and r.currency_code='PLN' THEN ((r.maximum_budget_minor_units/100)/4.4)
                WHEN r.country = 'PL' and r.currency_code<>'PLN' THEN (r.maximum_budget_minor_units/100)
                else (r.maximum_budget_minor_units/100) END) as maximum_budget
    FROM wkda_dm_retail_logistics.retail_ad AS ra
    left join wkda_dm_retail_logistics.refurbishment as r on r.retail_ad_id=ra.id
    WHERE 
    1
    and r.id is not null
    and r.state not in ('CANCELLED')
    GROUP BY 1
    ),



Num_per_car_ad as(

select 
    date_part(year, td.refurb_auth_date) as year,
    date_part(month, td.refurb_auth_date) as month,
    ra.retail_country,
    ra.stock_number,
    damage_reduced.entry_check_damages as damage_per_car,
    damage_reduced.admin_imperfections,
     
    count(distinct ra.stock_number) as num_cars_entry_check,
    count(dr.entry_check_damages) as num_damages_ec,
    count(dr.entry_check_imperfections) as num_imperfection_ec,
    count(dr.admin_damages) as num_damages_ad,
    count(dr.admin_imperfections) as num_imperfection_ad
    
    from wkda_dm_retail_logistics.retail_ad as ra
    left join first_ec as fe on ra.stock_number= fe.stock_number
    left join transition_date as td on ra.refurbishment_id= td.refurbishment_id
    left join damage_reduced as dr on ra.stock_number=dr.stock_number

    where 1 
    and ra.retail_country='DE'
    and year=2021
    AND ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    group by 1,2,3,4
    order by month 
    )
    
----------------------------------------------------------------------------------------



with -- first entry check
first_ec as (
    select * from 
    
        (select
        ra.stock_number,
        r.id as refurbishment_id,
        r.branch_name as name,
        rank() OVER (PARTITION by ra.stock_number order by r.created_on ASC) AS ranking
        
        from wkda_retail_refurbishment.refurbishment r
        left join wkda_dm_retail_logistics.retail_ad as ra on r.retail_ad_id=ra.id 
        
        where 1
        and r.state <> 'CANCELLED'
        and (r.completed_reason is null or r.completed_reason = 'RETAIL_READY')
        )
    where ranking = 1
),



transition_dates as (
    select
    stock_number,
    refurbishment_id,
    min(open_date) as open_date,
    min(refurb_ordered_date) as refurb_ordered_date,
    min(car_arrived_in_workshop) as car_arrived_in_workshop,
    min(car_in_buffer) as car_in_buffer,
    min(prepared_for_entry_check) as prepared_for_entry_check,
    min(prep_start_date) as prep_start_date,
    min(refurb_feedback_date) as refurb_feedback_date,
    min(refurb_auth_date) as refurb_auth_date,
    min(ready_for_cost_cald_cate) as ready_for_cost_cald_cate,
    min(refurb_start_date) as refurb_start_date,
    min(refurb_completed_date) as refurb_completed_date,
    min(refurb_qa_order_date) as refurb_qa_order_date,
    min(refurb_qa_completed_date) as refurb_qa_completed_date,
    min(completed_date) as completed_date,
    min(cancelled_date) as cancelled_date
    from (
        select
        ra.stock_number,
        rt.refurbishment_id,
        case when state_to = 'OPEN' then date(transition_date) end as open_date,
        case when state_to = 'REFURBISHMENT_ORDERED' then date(transition_date) end as refurb_ordered_date,
        case when state_to = 'CAR_ARRIVED_IN_WORKSHOP' then date(transition_date) end as car_arrived_in_workshop,
        case when state_to = 'CAR_IN_BUFFER' then date(transition_date) end as car_in_buffer,
        case when state_to = 'PREPARED_FOR_ENTRY_CHECK' then date(transition_date) end as prepared_for_entry_check,
        case when state_to = 'PREPARATION_STARTED' then date(transition_date) end as prep_start_date,
        case when state_to = 'READY_FOR_COST_CALCULATION' then date(transition_date) end as ready_for_cost_cald_cate,
        case when state_to = 'REFURBISHMENT_FEEDBACK_RECEIVED' then date(transition_date) end as refurb_feedback_date,
        case when state_to = 'REFURBISHMENT_AUTHORIZED' then date(transition_date) end as refurb_auth_date,
        case when state_to = 'REFURBISHMENT_STARTED' then date(transition_date) end as refurb_start_date,
        case when state_to = 'REFURBISHMENT_COMPLETED' then date(transition_date) end as refurb_completed_date,
        case when state_to = 'QUALITY_CHECK_ORDERED' then date(transition_date) end as refurb_qa_order_date,
        case when state_to = 'QUALITY_CHECK_COMPLETED' then date(transition_date) end as refurb_qa_completed_date,
        case when state_to = 'COMPLETED' then date(transition_date) end as completed_date,
        case when state_to = 'CANCELLED' then date(transition_date) end as cancelled_date
            
        from wkda_dm_retail_logistics.refurbishment_transition rt
        left join wkda_retail_refurbishment.refurbishment as r on rt.refurbishment_id = r.id
        left join wkda_retail_ad.retail_ad as ra on r.retail_ad_id=ra.id
        
        where r.cancel_reason is null
        and r.state <> 'CANCELLED'
        and ra.stock_number is not NULL
        AND ra.is_test = 'false'
        and ra.state not in ('RETURN_TO_AUTO1')
    )
    group by stock_number, refurbishment_id
),



-- car level KPIs (damages)
damage_reduced as (
    select
    service_name,
    ra.stock_number,
    sum(case when rs.service_name <>'NO_REPAIR_NEEDED' 
        and rs.state = 'PERFORMED' 
        and service_name is not null 
        and rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') 
            then 1 else null end) as entry_check_damages, -- bo damage
             
    sum(case when rs.service_name = 'NO_REPAIR_NEEDED' 
        and rad.display_to_customer_frontend = 'true' 
        and rad.source_type in ('ENTRY_CHECK_SUBMIT','ENTRY_CHECK_AUTHORIZE') 
            then 1 else null end) as entry_check_imperfections, -- bo imperfections
    
    sum(case when service_name = 'NO_REPAIR_NEEDED' 
    and rad.display_to_customer_frontend = 'true' and 
    rad.source_type in ('IMPORT') then 1 else null end) as admin_imperfections, -- adm_source_nrn 
    
    sum ( case when service_name <>'NO_REPAIR_NEEDED' and rs.state = 'PERFORMED' 
    and service_name is not null and rad.source_type in ('IMPORT') then 1 else null end ) 
    as admin_damages --adm_source_rn


    FROM wkda_dm_retail_logistics.retail_ad AS ra
    left join wkda_retail_ad.retail_ad_damage as rad on rad.retail_ad_id = ra.id
    left join wkda_retail_refurbishment.refurbishment_service as rs on rs.retail_ad_damage_id = rad.id
    
    where
    1
    and ra.is_test = 'false'
    -- and brake_damage is null
    group by ra.stock_number,service_name

),




num_cars_ad as (
    select
    date_part(year, td.refurb_auth_date) as year,
    date_part(month, td.refurb_auth_date) as month,
    ra.retail_country,
    --ra.stock_number,
    
    --damage_reduced.entry_check_damages as damage_per_car,
    --damage_reduced.admin_imperfections,
    count( ra.stock_number) as num_cars_entry_check,
    --count(distinct ra.stock_number) as num_cars_entry_check,
    avg(COALESCE(dr.entry_check_damages,0)) as avg_damages_ec,
    avg(COALESCE(dr.entry_check_imperfections,0)) as avg_imperfections_ec,
    avg(COALESCE(dr.admin_damages,0) ) as avg_damages_ad,
    avg(COALESCE(dr.admin_imperfections,0)) as avg_imperfections_ad,
    ---------------------------------------------------------------------------------


// New backup from damage_imperfections

   -- avg(damage_reduced.entry_check_damages) as avg_damages_ec,
   -- avg(damage_reduced.entry_check_imperfections) as avg_imperfections_ec,
   -- avg(damage_reduced.admin_damages ) as avg_damages_ad,
   -- avg(damage_reduced.admin_imperfections) as avg_imperfections_ad,
    
   -- case when damage_reduced.entry_check_damages<> null then avg(damage_reduced.entry_check_damages) end as avg_damages_ec,
   -- case when damage_reduced.entry_check_imperfections <> null then  avg(damage_reduced.entry_check_imperfections) end as avg_imperfections_ec,
   -- case when damage_reduced.admin_damages <> null then   avg(damage_reduced.admin_damages ) end as avg_damages_ad,
   -- case when damage_reduced.admin_imperfections<> null then avg(damage_reduced.admin_imperfections) end as avg_imperfections_ad
    
    
    sum(dr.entry_check_damages) as Total_damages_ec,
    sum(dr.entry_check_imperfections) as Total_imperfections_ec,
    sum(dr.admin_damages) as Total_damages_ad,
    sum(dr.admin_imperfections) as Total_imperfections_ad
    
    
    from wkda_dm_retail_logistics.retail_ad as ra
    left join first_ec as fe on ra.stock_number = fe.stock_number
    left join transition_dates as td on td.refurbishment_id=fe.refurbishment_id
    left join damage_reduced as dr on ra.stock_number=dr.stock_number
   --left join damage_reduced on ra.stock_number = damage_reduced.stock_number

    where 1
    and ra.retail_country='DE'
    and year=2021
    AND ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    group by 1,2,3
    order by month 
    
    )

 

 ,   
/*select 

 stock_number,
 count (damage_reduced.entry_check_damages) as num_damages_ec_percar

from damage_reduced

group by 1*/


Num_per_car_ad as(

select 
    date_part(year, td.refurb_auth_date) as year,
    date_part(month, td.refurb_auth_date) as month,
    ra.retail_country,
    ra.stock_number,
    dr.entry_check_damages as ec_damage_per_car,
    dr.entry_check_imperfections as ec_imperfection_per_car,
    dr.admin_damages as ad_damage_per_car,
    dr.admin_imperfections as ad_imperfections
     
    --count(distinct ra.stock_number) as num_cars_entry_check,
    --count(dr.entry_check_damages) as num_damages_ec,
   -- count(dr.entry_check_imperfections) as num_imperfection_ec,
   -- count(dr.admin_damages) as num_damages_ad,
   -- count(dr.admin_imperfections) as num_imperfection_ad
    
    
    from wkda_dm_retail_logistics.retail_ad as ra
    left join first_ec as fe on ra.stock_number = fe.stock_number
    left join transition_dates as td on td.refurbishment_id=fe.refurbishment_id
    left join damage_reduced as dr on ra.stock_number=dr.stock_number
    

    where 1 
    and ra.retail_country='DE'
    and year=2021
    AND ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    group by 1,2,3,4,5,6,7,8
    order by month 
    )
    
select * from Num_per_car_ad

--SELECT * FROM num_cars_ad where 1 and  stock_number = '{{stock_number}}';


-----------------------------------------------------------num_cars_ad as (
    select
    date_part(year, td.refurb_auth_date) as year,
    date_part(month, td.refurb_auth_date) as month,
    ra.retail_country,
    --ra.stock_number,
    
    --damage_reduced.entry_check_damages as damage_per_car,
    --damage_reduced.admin_imperfections,
    count( ra.stock_number) as num_cars_entry_check,
    --count(distinct ra.stock_number) as num_cars_entry_check,
    avg(COALESCE(dr.entry_check_damages,0)) as avg_damages_ec,
    avg(COALESCE(dr.entry_check_imperfections,0)) as avg_imperfections_ec,
    avg(COALESCE(dr.admin_damages,0) ) as avg_damages_ad,
    avg(COALESCE(dr.admin_imperfections,0)) as avg_imperfections_ad,
    ---------------------------------------------------------------------------------
   -- avg(damage_reduced.entry_check_damages) as avg_damages_ec,
   -- avg(damage_reduced.entry_check_imperfections) as avg_imperfections_ec,
   -- avg(damage_reduced.admin_damages ) as avg_damages_ad,
   -- avg(damage_reduced.admin_imperfections) as avg_imperfections_ad,
    
   -- case when damage_reduced.entry_check_damages<> null then avg(damage_reduced.entry_check_damages) end as avg_damages_ec,
   -- case when damage_reduced.entry_check_imperfections <> null then  avg(damage_reduced.entry_check_imperfections) end as avg_imperfections_ec,
   -- case when damage_reduced.admin_damages <> null then   avg(damage_reduced.admin_damages ) end as avg_damages_ad,
   -- case when damage_reduced.admin_imperfections<> null then avg(damage_reduced.admin_imperfections) end as avg_imperfections_ad
    
    
    sum(dr.entry_check_damages) as Total_damages_ec,
    sum(dr.entry_check_imperfections) as Total_imperfections_ec,
    sum(dr.admin_damages) as Total_damages_ad,
    sum(dr.admin_imperfections) as Total_imperfections_ad
    
    
    from wkda_dm_retail_logistics.retail_ad as ra
    left join first_ec as fe on ra.stock_number = fe.stock_number
    left join transition_dates as td on td.refurbishment_id=fe.refurbishment_id
    left join damage_reduced as dr on ra.stock_number=dr.stock_number
   --left join damage_reduced on ra.stock_number = damage_reduced.stock_number

    where 1
    and ra.retail_country='DE'
    and year=2021
    AND ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    group by 1,2,3
    order by month 
    
    )

 


