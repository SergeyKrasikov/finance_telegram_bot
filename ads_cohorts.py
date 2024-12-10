from datetime import date, timedelta, datetime
import logging
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

def extract_appsflyer(ch_conn_id :str, pg_conn_id :str, **context) -> None:
    ch_hook = ClickHouseHook(ch_conn_id)
    pg_hook = PostgresHook(pg_conn_id)

   
    query = f"""
            with idlist as (
                select distinct advertising_id
                from appsflyer.installs 
                WHERE event_time >= '2024-06-01' and advertising_id not in ('0000-0000', '00000000-0000-0000-0000-000000000000') and advertising_id is not null
                
            ),

            inap as (
                select event_time, advertising_id, idfa, appsflyer_id, event_revenue_eur, event_revenue_usd, event_value, event_name, bundle_id
                from appsflyer.inapps_eur 
                inner join idlist using advertising_id
            ),

            ads_at as (
                select event_time, advertising_id, idfa, appsflyer_id, event_revenue_eur, bundle_id
                from appsflyer.attributed_ad_revenue_eur
                inner join idlist using advertising_id
            ),

            ads_org as (
                select event_time, advertising_id, idfa, appsflyer_id, event_revenue_eur, bundle_id
                from appsflyer.attributed_ad_revenue_eur
                inner join idlist using advertising_id
            ),

            inst as (
                select event_time, advertising_id, idfa, appsflyer_id, bundle_id, media_source, campaign, af_c_id, af_adset, af_adset_id, country_code
                from appsflyer.installs 
                inner join idlist using advertising_id
            ),

            union_data as (
                select 
                event_time, 
                ifNull(ifNull(advertising_id, idfa), appsflyer_id) as id,
                toFloat64OrNull(event_revenue_eur) as rev_gross, 
                toFloat64OrNull(JSONExtractString(event_value, 'af_net_revenue_usd')) * (toFloat64OrNull(event_revenue_eur)/toFloat64OrNull(event_revenue_usd)) as rev_net,
                'inapp' AS revtype,
                null as game,
                null as media_source,
                null as campaign,
                null as af_c_id,
                null as af_adset,
                null as af_adset_id,
                null as country
                from inap
                where event_name in ('End_session', 'af_purchase', 'Start_mini_game') and bundle_id not in ('com.dominigames.mf', 'com.dominigames.mf.ios.free2play', 'ru.dominigames.mf-RuStore', 'com.dominigames.mf.ios.prem')
                
                union all
                
                select 
                date_add(DAY, 1, event_time) as event_time,
                ifNull(ifNull(advertising_id, idfa), appsflyer_id) as id,
                toFloat64OrNull(event_revenue_eur) as rev_gross, 
                toFloat64OrNull(event_revenue_eur) as rev_net,
                'ads' AS revtype,
                null as game,
                null as media_source,
                null as campaign,
                null as af_c_id,
                null as af_adset,
                null as af_adset_id,
                null as country
                from ads_at
                where event_revenue_eur is not null and bundle_id not in ('com.dominigames.mf', 'com.dominigames.mf.ios.free2play', 'ru.dominigames.mf-RuStore', 'com.dominigames.mf.ios.prem')
                
                union all
                
                select 
                date_add(DAY, 1, event_time) as event_time,
                ifNull(ifNull(advertising_id, idfa), appsflyer_id) as id,
                toFloat64OrNull(event_revenue_eur) as rev_gross, 
                toFloat64OrNull(event_revenue_eur) as rev_net,
                'ads' AS revtype,
                null as game,
                null as media_source,
                null as campaign,
                null as af_c_id,
                null as af_adset,
                null as af_adset_id,
                null as country
                FROM ads_org
                where event_revenue_eur is not null and bundle_id not in ('com.dominigames.mf', 'com.dominigames.mf.ios.free2play', 'ru.dominigames.mf-RuStore', 'com.dominigames.mf.ios.prem')
                
                union all
                
                select 
                event_time,
                    ifNull(ifNull(advertising_id, idfa), appsflyer_id) as id,
                    null as rev_gross,
                    null as rev_net,
                    'install' as revtype,
                    bundle_id as game,
                media_source,
                ifNull(campaign, 'organic') as campaign,
                ifNull(af_c_id, 'organic') as af_c_id,
                ifNull(af_adset, 'organic') as af_adset,
                ifNull(af_adset_id, 'organic') as af_adset_id,
                    CASE 
                        WHEN country_code = 'AE' THEN 'United Arab Emirates'
                        WHEN country_code = 'AT' THEN 'Austria'
                        WHEN country_code = 'AU' THEN 'Australia'
                        WHEN country_code = 'BE' THEN 'Belgium'
                        WHEN country_code = 'CA' THEN 'Canada'
                        WHEN country_code = 'CH' THEN 'Switzerland'
                        WHEN country_code = 'DE' THEN 'Germany'
                        WHEN country_code = 'DK' THEN 'Denmark'
                        WHEN country_code = 'ES' THEN 'Spain'
                        WHEN country_code = 'FI' THEN 'Finland'
                        WHEN country_code = 'FR' THEN 'France'
                        WHEN country_code = 'HK' THEN 'Hong Kong'
                        WHEN country_code = 'IE' THEN 'Ireland'
                        WHEN country_code = 'IT' THEN 'Italy'
                        WHEN country_code = 'JP' THEN 'Japan'
                        WHEN country_code = 'KR' THEN 'South Korea'
                        WHEN country_code = 'NL' THEN 'Netherlands'
                        WHEN country_code = 'NO' THEN 'Norway'
                        WHEN country_code = 'NZ' THEN 'New Zealand'
                        WHEN country_code = 'PL' THEN 'Poland'
                        WHEN country_code = 'SA' THEN 'Saudi Arabia'
                        WHEN country_code = 'SE' THEN 'Sweden'
                        WHEN country_code = 'SG' THEN 'Singapore'
                        WHEN country_code = 'UK' THEN 'United Kingdom'
                        WHEN country_code = 'US' THEN 'United States'
                        WHEN country_code = 'ZA' THEN 'South Africa'
                        ELSE 'Other' 
                    END AS country
                from inst
                where bundle_id not in ('com.dominigames.mf', 'com.dominigames.mf.ios.free2play', 'ru.dominigames.mf-RuStore', 'com.dominigames.mf.ios.prem')),

            full_prep_data as (
                select 
                    *
                    , (event_time-prev_event_timestamp)/3600 as hours_difference
                from (
                    select 
                        *
                        , any(event_time) over(partition by id order by event_time rows between 1 preceding and 1 preceding) as prev_event_timestamp                   
                        , count(case when revtype = 'install' then event_time else null end) over(partition by id order by event_time) as install_count
                        , count(case when revtype = 'install' and af_c_id <> 'organic' then event_time else null end) over(partition by id order by event_time) as ad_install_count
                    from union_data)),	
                    
            reatribut_3day as (
                SELECT
                    'reatribut_3day' as reatribut
                    ,date_trunc('day', any_install_time) as install_day
                    ,any_game ,any_mediasource ,any_campaign ,any_af_c_id ,any_country
                    ,uniqExact(id) as id_all
                    ,uniqExact(case when ((event_time-any_install_time)/3600)<24 then id else null end) as id_1d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=24 and ((event_time-any_install_time)/3600) < 48 then id else null end) as id_2d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=48 and ((event_time-any_install_time)/3600) < 72 then id else null end) as id_3d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=144 and ((event_time-any_install_time)/3600) < 168 then id else null end) as id_7d
                    ,sum(case when revtype='inapp' then rev_gross end) as inapp_rev_gross_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_30d
                    ,sum(case when revtype='inapp' then rev_net end) as inapp_rev_net_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_net else null end) as inapp_rev_net_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_net else null end) as inapp_rev_net_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_net else null end) as inapp_rev_net_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_net else null end) as inapp_rev_net_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_net else null end) as inapp_rev_net_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_net else null end) as inapp_rev_net_30d
                    ,sum(case when revtype='ads' then rev_gross end) as ads_rev_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='ads' then rev_gross else null end) as ads_rev_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='ads' then rev_gross else null end) as ads_rev_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='ads' then rev_gross else null end) as ads_rev_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='ads' then rev_gross else null end) as ads_rev_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='ads' then rev_gross else null end) as ads_rev_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='ads' then rev_gross else null end) as ads_rev_30d
                from (
                    select 
                        event_time, id, rev_gross, rev_net, revtype
                        ,anyLast(case when revtype = 'install' then event_time else null end) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_install_time
                        ,anyLast(game) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_game
                        ,anyLast(media_source) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_mediasource
                        ,anyLast(campaign) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_campaign
                        ,anyLast(af_c_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_c_id
                        ,anyLast(af_adset) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset
                        ,anyLast(af_adset_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset_id
                        ,anyLast(country) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_country
                    from full_prep_data
                    where 
                        not((ad_install_count>1 and revtype='install' and hours_difference<72 and af_c_id <> 'organic')
                        or (install_count>1 and revtype='install' and hours_difference<72 and af_c_id = 'organic')))
                group by reatribut,install_day,any_game ,any_mediasource ,any_campaign ,any_af_c_id, any_country),

            reatribut_7day as (
                SELECT
                    'reatribut_7day' as reatribut
                    ,date_trunc('day', any_install_time) as install_day
                    ,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country
                    ,uniqExact(id) as id_all
                    ,uniqExact(case when ((event_time-any_install_time)/3600)<24 then id else null end) as id_1d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=24 and ((event_time-any_install_time)/3600) < 48 then id else null end) as id_2d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=48 and ((event_time-any_install_time)/3600) < 72 then id else null end) as id_3d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=144 and ((event_time-any_install_time)/3600) < 168 then id else null end) as id_7d
                    ,sum(case when revtype='inapp' then rev_gross end) as inapp_rev_gross_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_30d
                    ,sum(case when revtype='inapp' then rev_net end) as inapp_rev_net_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_net else null end) as inapp_rev_net_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_net else null end) as inapp_rev_net_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_net else null end) as inapp_rev_net_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_net else null end) as inapp_rev_net_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_net else null end) as inapp_rev_net_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_net else null end) as inapp_rev_net_30d
                    ,sum(case when revtype='ads' then rev_gross end) as ads_rev_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='ads' then rev_gross else null end) as ads_rev_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='ads' then rev_gross else null end) as ads_rev_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='ads' then rev_gross else null end) as ads_rev_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='ads' then rev_gross else null end) as ads_rev_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='ads' then rev_gross else null end) as ads_rev_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='ads' then rev_gross else null end) as ads_rev_30d
                from (
                    select 
                        event_time, id, rev_gross, rev_net, revtype
                        ,anyLast(case when revtype = 'install' then event_time else null end) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_install_time
                        ,anyLast(game) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_game
                        ,anyLast(media_source) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_mediasource
                        ,anyLast(campaign) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_campaign
                        ,anyLast(af_c_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_c_id
                        ,anyLast(af_adset) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset
                        ,anyLast(af_adset_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset_id
                        ,anyLast(country) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_country
                    from full_prep_data
                    where 
                        not((ad_install_count>1 and revtype='install' and hours_difference<168 and af_c_id <> 'organic')
                        or (install_count>1 and revtype='install' and hours_difference<168 and af_c_id = 'organic')))
                group by reatribut,install_day,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country),
                
            reatribut_every as (
                SELECT
                    'reatribut_every' as reatribut
                    ,date_trunc('day', any_install_time) as install_day
                    ,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country
                    ,uniqExact(id) as id_all
                    ,uniqExact(case when ((event_time-any_install_time)/3600)<24 then id else null end) as id_1d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=24 and ((event_time-any_install_time)/3600) < 48 then id else null end) as id_2d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=48 and ((event_time-any_install_time)/3600) < 72 then id else null end) as id_3d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=144 and ((event_time-any_install_time)/3600) < 168 then id else null end) as id_7d
                    ,sum(case when revtype='inapp' then rev_gross end) as inapp_rev_gross_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_30d
                    ,sum(case when revtype='inapp' then rev_net end) as inapp_rev_net_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_net else null end) as inapp_rev_net_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_net else null end) as inapp_rev_net_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_net else null end) as inapp_rev_net_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_net else null end) as inapp_rev_net_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_net else null end) as inapp_rev_net_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_net else null end) as inapp_rev_net_30d
                    ,sum(case when revtype='ads' then rev_gross end) as ads_rev_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='ads' then rev_gross else null end) as ads_rev_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='ads' then rev_gross else null end) as ads_rev_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='ads' then rev_gross else null end) as ads_rev_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='ads' then rev_gross else null end) as ads_rev_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='ads' then rev_gross else null end) as ads_rev_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='ads' then rev_gross else null end) as ads_rev_30d
                from (
                    select 
                        event_time, id, rev_gross, rev_net, revtype
                        ,anyLast(case when revtype = 'install' then event_time else null end) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_install_time
                        ,anyLast(game) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_game
                        ,anyLast(media_source) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_mediasource
                        ,anyLast(campaign) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_campaign
                        ,anyLast(af_c_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_c_id
                        ,anyLast(af_adset) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset
                        ,anyLast(af_adset_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset_id
                        ,anyLast(country) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_country
                    from full_prep_data
                    where 
                        not((install_count>1 and revtype='install' and af_c_id = 'organic')))
                group by reatribut, install_day,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country),

            reatribut_firstonly as (
                SELECT
                    'reatribut_firstonly' as reatribut
                    ,date_trunc('day', any_install_time) as install_day
                    ,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country
                    ,uniqExact(id) as id_all
                    ,uniqExact(case when ((event_time-any_install_time)/3600)<24 then id else null end) as id_1d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=24 and ((event_time-any_install_time)/3600) < 48 then id else null end) as id_2d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=48 and ((event_time-any_install_time)/3600) < 72 then id else null end) as id_3d
                    ,uniqExact(case when ((event_time-any_install_time)/3600)>=144 and ((event_time-any_install_time)/3600) < 168 then id else null end) as id_7d
                    ,sum(case when revtype='inapp' then rev_gross end) as inapp_rev_gross_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_gross else null end) as inapp_rev_gross_30d
                    ,sum(case when revtype='inapp' then rev_net end) as inapp_rev_net_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='inapp' then rev_net else null end) as inapp_rev_net_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='inapp' then rev_net else null end) as inapp_rev_net_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='inapp' then rev_net else null end) as inapp_rev_net_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='inapp' then rev_net else null end) as inapp_rev_net_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='inapp' then rev_net else null end) as inapp_rev_net_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='inapp' then rev_net else null end) as inapp_rev_net_30d
                    ,sum(case when revtype='ads' then rev_gross end) as ads_rev_all
                    ,sum(case when ((event_time-any_install_time)/3600)<24 and revtype='ads' then rev_gross else null end) as ads_rev_1d
                    ,sum(case when ((event_time-any_install_time)/3600)<48 and revtype='ads' then rev_gross else null end) as ads_rev_2d
                    ,sum(case when ((event_time-any_install_time)/3600)<72 and revtype='ads' then rev_gross else null end) as ads_rev_3d
                    ,sum(case when ((event_time-any_install_time)/3600)<168 and revtype='ads' then rev_gross else null end) as ads_rev_7d
                    ,sum(case when ((event_time-any_install_time)/3600)<336 and revtype='ads' then rev_gross else null end) as ads_rev_14d
                    ,sum(case when ((event_time-any_install_time)/3600)<720 and revtype='ads' then rev_gross else null end) as ads_rev_30d
                from (
                    select 
                        event_time, id, rev_gross, rev_net, revtype
                        ,anyLast(case when revtype = 'install' then event_time else null end) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_install_time
                        ,anyLast(game) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_game
                        ,anyLast(media_source) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_mediasource
                        ,anyLast(campaign) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_campaign
                        ,anyLast(af_c_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_c_id
                        ,anyLast(af_adset) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset
                        ,anyLast(af_adset_id) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_af_adset_id
                        ,anyLast(country) over(partition by id order by event_time rows between UNBOUNDED preceding and CURRENT row) as any_country
                    from full_prep_data
                    where 
                        not((ad_install_count>1 and revtype='install' and af_c_id <> 'organic')
                        or (install_count>1 and revtype='install' and af_c_id = 'organic')))
                group by reatribut,install_day,any_game ,any_mediasource ,any_campaign ,any_af_c_id,any_country)	


            select *
            from reatribut_firstonly
            UNION ALL
            SELECT *
            from
            reatribut_3day
            UNION ALL
            SELECT *
            from
            reatribut_7day
            UNION ALL
            SELECT *
            from
            reatribut_every
        """
    
    result =  ch_hook.get_pandas_df(query)

        
    #pg_hook.run("delete from appsflyer_limit")
    
    engine = pg_hook.get_sqlalchemy_engine()
    result.to_sql('ads_cohorts', con=engine, if_exists='append', index=False)

with DAG(
    dag_id='ads_cohorts',
    start_date=datetime(2023, 9, 20),
    schedule_interval='00 12 * * *',
    catchup=False,
    default_args={'owner': 'Максим'}
) as dag:
    extract_appsflyer_task = PythonOperator(
    task_id='extract_appsflyer_task',
    python_callable=extract_appsflyer,
    op_kwargs={
        'ch_conn_id': 'clickhouse_firebase_events_pandas',
        'pg_conn_id': 'postgres_dominigames'
    }
    )
extract_appsflyer_task