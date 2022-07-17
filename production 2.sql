with retail_ad_no_test_no_return as (
    select *
    from wkda_retail_ad.retail_ad as ra
    where 1
    and ra.is_test = 'false'
    and ra.state not in ('RETURN_TO_AUTO1')
    -- and ra.auto1return_reason is NULL
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

booked_date as (
    select ra.stock_number,
    nvl(cs.b2b_deal_datetime,ra.first_import_on) as booked_date

    from retail_ad_no_test_no_return as ra
    left join wkda_dm_retail_logistics.car_leads as cl on cl.stock_number = ra.stock_number
    left join wkda_dm_retail_logistics.car_sales as cs on cs.id = cl.id
),

first_tour as ( 
    select
    stock_number,
    refurbishment_id,
    completed_date as first_retail_ready,
    total_cost_budget_minor_units,
    maximum_budget_minor_units,
    branch_name

    from (
        select
        ra.stock_number,
        r.id as refurbishment_id,
        ra.first_import_on,
        date(rt.transition_date) as completed_date,
        r.total_cost_budget_minor_units,
        r.maximum_budget_minor_units,
        r.branch_name,
        rank() OVER (PARTITION by ra.stock_number order by date(r.created_on)) AS ranking
        
        from retail_ad_no_test_no_return as ra
        join wkda_retail_refurbishment.refurbishment r on r.retail_ad_id=ra.id
        join wkda_retail_refurbishment.refurbishment_transition as rt on rt.refurbishment_id=r.id
        
        where r.cancel_reason is null
        and r.completed_reason = 'RETAIL_READY'
        AND r.state = 'COMPLETED'
        and rt.state_to = 'COMPLETED'
        
    )
    
    where ranking = 1
),

service_budget_corrected as (

    select 
    ra.retail_country,
    -- rs.created_on as rs_created_on,
    r.id as refurb_id,
    r.state as refurb_state,
    r.branch_name as refurb_location,
    ft.refurbishment_id as rr_refurb_id,
    ra.stock_number,
    ft.first_retail_ready,
    bd.booked_date,
    rs.service_type,
    rs.service_name,
    rs.state as service_state,
    rs.id as service_id,
    rs.comment as service_comment,
    ft.branch_name as rr_branch_name,
    rad.part as damage_part,
    rad.sub_area as damage_sub_area,
    rad.area as damage_area,
    rad.type as damage_type,
    rs.comment as comment,
    json_extract_path_text(ca.vehicle, 'modelApi', 'make', true) as car_make,
    json_extract_path_text(ca.vehicle, 'modelApi', 'model', true) as car_model,
    
    case when r.country not in ('PL','SE') then 1
    when r.currency_code='PLN' then 4.4
    when r.country = 'PL' and r.currency_code<>'PLN' then 1
    when r.country = 'SE' then 10.3 end as conversion,
    
    ((r.total_cost_budget_minor_units/100)/conversion)::decimal(7,2) as total_cost_budget,
    ((r.maximum_budget_minor_units/100)/conversion)::decimal(7,2) as max_budget,
    ((rs.budget_minor_units/100)/conversion)::decimal(7,2) as service_budget_corrected,
    r.latest_rework_date
 
    
    from retail_ad_no_test_no_return as ra
    left join first_tour as ft on ra.stock_number=ft.stock_number
    left join wkda_retail_refurbishment.refurbishment r on r.retail_ad_id=ra.id
    left join wkda_retail_refurbishment.refurbishment_service rs on rs.refurbishment_id=r.id
    left join wkda_retail_ad.retail_ad_damage as rad on rs.retail_ad_damage_id=rad.id
    
    left join booked_date as bd on ra.stock_number=bd.stock_number
    left join wkda_classifieds.ad as ca on ca.id=ra.ad_id
)



select * from service_budget_corrected as sbc

where 1
and retail_country='DE'
and first_retail_ready > current_date - interval '3 months'

