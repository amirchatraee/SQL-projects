transition_dates as (
    select
    stock_number,
    refurbishment_id,
    last_transition_date,
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
        r.updated_on as last_transition_date,
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
    group by stock_number, refurbishment_id, last_transition_date
    ),
    
final as ( 

 select 
 td.stock_number,
 refurb_ordered_date as refurb_created_date,
 cast(last_transition_date as date),
 td.refurbishment_id,
 min(open_email) as open_email,
 min(refurb_ordered_email) as refurb_ordered_email,
 min(car_arrived_in_workshop_email) as car_arrived_in_workshop_email,
 min(car_in_buffer_email) as car_in_buffer_email,
 min(prepared_for_entry_check_email) as prepared_for_entry_check_email,
 min(prep_start_email) as prep_start_email,
 min(ready_for_cost_calc_email) as ready_for_cost_calc_email,
 min(refurb_feedback_email) as refurb_feedback_email,
 min(refurb_auth_email) as refurb_auth_email,
 min(refurb_start_email) as refurb_start_email,
 min(refurb_completed_email) as refurb_completed_email,
 min(refurb_qa_order_email) as refurb_qa_order_email,
 min(refurb_qa_completed_email) as refurb_qa_completed_email,
 min(completed_email) as completed_email,
 min(cancelled_email) as cancelled_email

 
 from transition_dates as td
 left join transition_email as tm on tm.stock_number=td.stock_number
 group by 1,2,3,4
 
 )
 
 
 
 
select * 

from final 

where  refurb_created_date >= getdate() - interval '6 months'
order by date


