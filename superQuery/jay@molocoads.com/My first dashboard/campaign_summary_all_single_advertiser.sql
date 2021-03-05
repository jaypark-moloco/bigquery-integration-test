CREATE OR REPLACE PROCEDURE `moloco-data-prod.looker.merge_campaign_summary_all_single_advertiser`(IN date_run DATE,IN date_lookback DATE,IN advertiser_name STRING) 
BEGIN
------------------------------------------------------------------------------------------------------------------------

DECLARE table_str DATE DEFAULT DATE_ADD(date_run, INTERVAL 1 DAY);
DECLARE table_lookback DATE DEFAULT DATE_ADD(date_lookback, INTERVAL -1 DAY);

MERGE `moloco-data-prod.looker.campaign_summary_all` T
USING (
WITH title_lookup_table AS (
      SELECT
          platform,
          advertiser_title,
          campaign_title,
          campaign_id,
          CASE
            WHEN advertiser_title IS NULL THEN advertiser_id
            WHEN advertiser_title = '' THEN advertiser_id
            WHEN advertiser_id =  advertiser_title THEN advertiser_id
            ELSE concat(advertiser_title, "#", advertiser_id)
          END AS advertiser,
          CASE
            WHEN campaign_title IS NULL THEN campaign_id
            WHEN campaign_title = '' THEN campaign_id
            WHEN campaign_id = campaign_title THEN campaign_id
            ELSE concat(campaign_title, "#", campaign_id)
          END AS campaign
         FROM `focal-elf-631.standard_digest.title_lookup_table`
         WHERE advertiser_id = advertiser_name
         GROUP BY 1,2,3,4,5,6)
     , campaign_digest_merged_latest AS (
       SELECT 
          t1.*,
          t2.Category AS category_content
       FROM (SELECT
                platform_name,
                advertiser_name AS advertiser_id,
                campaign_name AS campaign_id,
                product_name,
                product_display_name,
                tracking_bundle,
                country,
                product_category
            FROM `focal-elf-631.prod.campaign_digest_merged_latest`
            WHERE advertiser_name = advertiser_name
            GROUP BY 1,2,3,4,5,6,7,8) t1 
      LEFT JOIN `focal-elf-631.common.app_category_iab` t2
      ON t1.product_category = t2.Criterion_ID)
     , tcb_table AS (
       SELECT 
          t1.*,
          t2.first_launch,
          CASE
            WHEN t1.platform = 'NETMARBLE' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-12-27', DAY) > 90, 'Existing', 'New') --netmarble cloud
            WHEN t1.platform = 'NEXON' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-12-13', DAY) > 90, 'Existing', 'New') --nexon cloud
            WHEN t1.platform = 'BAGELCODE' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-04-16', DAY) > 90, 'Existing', 'New') --bagelcode cloud
            WHEN t1.platform = 'SMN_CORPORATION' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-08-12', DAY) > 90, 'Existing', 'New') --smn_corporation cloud
            WHEN t1.platform = '2K_Games' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-08-20', DAY) > 90, 'Existing', 'New') --2k_games cloud
            WHEN t1.platform = 'Unico_Studio' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-09-02', DAY) > 90, 'Existing', 'New') --Unico_Studio cloud
            WHEN t1.platform = 'WISEBIRDS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-08-19', DAY) > 90, 'Existing', 'New') --wisebirds cloud
            WHEN t1.platform = 'REMAKE_DIGITAL' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-08-21', DAY) > 90, 'Existing', 'New') --remake cloud
            WHEN t1.platform = 'CLOVERGAMES' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-09-11', DAY) > 90, 'Existing', 'New') --clovergames cloud
            WHEN t1.platform = 'GOOD_GAMES_STUDIO' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-10-25', DAY) > 90, 'Existing', 'New') --goodgamesstudio cloud
            WHEN t1.platform = 'ELEVATE_LABS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-09-23', DAY) > 90, 'Existing', 'New') --elevate cloud
            WHEN t1.platform = 'INNOGAMES' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-06-08', DAY) > 90, 'Existing', 'New') --innogames cloud
            WHEN t1.platform = 'DDIVE' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-10-05', DAY) > 90, 'Existing', 'New') --ddive cloud
            WHEN t1.platform = 'DEVSISTERS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-09-06', DAY) > 90, 'Existing', 'New') --devsisters cloud
            WHEN t1.platform = 'HYPERCONNECT' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-05-21', DAY) > 90, 'Existing', 'New') --hyperconnect cloud
            WHEN t1.platform = 'NODGAMES' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-10-22', DAY) > 90, 'Existing', 'New') --nodgames cloud
            WHEN t1.platform = 'SPACEODDITY' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-10-22', DAY) > 90, 'Existing', 'New') --spaceoddity cloud
            WHEN t1.platform = 'SPOONRADIO' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-10-28', DAY) > 90, 'Existing', 'New') --spoonradio cloud
            WHEN t1.platform = 'SUNDAYTOZ' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2017-03-30', DAY) > 90, 'Existing', 'New') --sundaytoz cloud
            WHEN t1.platform = 'INCROSS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-11-03', DAY) > 90, 'Existing', 'New') --incross cloud
            WHEN t1.platform = 'JAM_CITY' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-07-15', DAY) > 90, 'Existing', 'New') --jam_city cloud
            WHEN t1.platform = 'BITMANGO' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-04-30', DAY) > 90, 'Existing', 'New') --bitmango cloud
            WHEN t1.platform = 'PEARLABYSS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-11-11', DAY) > 90, 'Existing', 'New') --pearlabyss cloud
            WHEN t1.platform = 'MOBIDAYS' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-11-10', DAY) > 90, 'Existing', 'New') --mobidays cloud
            WHEN t1.platform = 'MADUP' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-11-13', DAY) > 90, 'Existing', 'New') --madup cloud
            WHEN t1.advertiser_id = 'xqbAkdpstujR58zD' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-06-08', DAY) > 90, 'Existing', 'New') --nexon i/c
            WHEN t1.advertiser_id = 'pRPSvVzVnqJDwUnV' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-12-27', DAY) > 90, 'Existing', 'New') --netmarble i/c
            WHEN t1.advertiser_id = 'SERYzZlCfN0Kk1OG' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-04-19', DAY) > 90, 'Existing', 'New') --joycity i/c
            WHEN t1.advertiser_id = 'BusdZiyjKshqhXSP' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-04-19', DAY) > 90, 'Existing', 'New') --joycity_dotlab i/c
            WHEN t1.advertiser_id = 'FTl9mrkw5Pssxa9u' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-11-25', DAY) > 90, 'Existing', 'New') --linegames i/c
            WHEN t1.advertiser_id = 'DVUjFzeMPIZCEqIj' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-07-14', DAY) > 90, 'Existing', 'New')  --playtike i/c
            WHEN t1.advertiser_id = 'GPdwFZ9fJvtK72dZ' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-08-27', DAY) > 90, 'Existing', 'New') --tiltingpoint i/c
            WHEN t1.advertiser_id = 'vyWbUrDP8r6DhKik' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-07-01', DAY) > 90, 'Existing', 'New') --commseed_coporation i/c
            WHEN t1.advertiser_id = 'xembc4n04sDR4uJX' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-12-27', DAY) > 90, 'Existing', 'New') --netmarble_team2 i/c
            WHEN t1.advertiser_id = 'KmBb6fZDnEuiEeqw' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-06-19', DAY) > 90, 'Existing', 'New') --enish i/c
            WHEN t1.advertiser_id = 'wA5CCRT9rFZKs4Zq' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2017-12-07', DAY) > 90, 'Existing', 'New') --mobilityware i/c
            WHEN t1.advertiser_id = 'vXzMeCfpxD4u6Frc' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-06-19', DAY) > 90, 'Existing', 'New') --glu i/c
            WHEN t1.advertiser_id = 'Xtd5CRR9aDSvMHfH' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-09-04', DAY) > 90, 'Existing', 'New') --winclap i/c
            WHEN t1.advertiser_id = 'WkNhMK1ZdEz4fp6X' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-01-05', DAY) > 90, 'Existing', 'New') --10x10 i/c
            WHEN t1.advertiser_id = 'T7XLb4PtEJWh9rbk' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-07-15', DAY) > 90, 'Existing', 'New') --jam_city i/c
            WHEN t1.advertiser_id = 'WAENHzYKzbzS0VoX' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-04-06', DAY) > 90, 'Existing', 'New') --nbt i/c
            WHEN t1.advertiser_id = 'bs2HXhhl9jiV8GIa' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-03-07', DAY) > 90, 'Existing', 'New') --pnix i/c
            WHEN t1.advertiser_id = 'mPnfQe0FEVgABIxY' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2017-02-02', DAY) > 90, 'Existing', 'New') --neowiz_ddive i/c
            WHEN t1.advertiser_id = 'asSCR51XuYUi3yI6' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-01-09', DAY) > 90, 'Existing', 'New') --gamepub i/c
            WHEN t1.advertiser_id = 'Tkqw6DzT4NsU721w' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-06-05', DAY) > 90, 'Existing', 'New') --bermuda i/c
            WHEN t1.advertiser_id = 'hyperconnect_slide' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2018-05-21', DAY) > 90, 'Existing', 'New') --hyperconnect i/c
            WHEN t1.advertiser_id = 'Ahy7diMQ6rRsxiz3' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-03-29', DAY) > 90, 'Existing', 'New') --reigntalk i/c
            WHEN t1.advertiser_id = 'qGn76zoT4QtVQ2i9' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-08-13', DAY) > 90, 'Existing', 'New') --kredivo i/c
            WHEN t1.advertiser_id = 'hW6oamQObaG3veeD' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-11-26', DAY) > 90, 'Existing', 'New') --arumgames i/c
            WHEN t1.advertiser_id = 'Ixmn1BBrDxc8eWeh' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2017-12-04', DAY) > 90, 'Existing', 'New') --naverwebtoon i/c
            WHEN t1.advertiser_id = 'VPzzC9vCByui3ARZ' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-07-09', DAY) > 90, 'Existing', 'New') --klab_irep i/c
            WHEN t1.advertiser_id = 'iOgonEoavZ1qfvy0' THEN  IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-06-15', DAY) > 90, 'Existing', 'New') --greencar i/c
            WHEN t1.advertiser_id = 'n8mX8ytk0iqotyyl' THEN  IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-04-15', DAY) > 90, 'Existing', 'New') --root i/c
            WHEN t1.advertiser_id = 'nqE268wigpn6Yqz0' THEN  IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-02-04', DAY) > 90, 'Existing', 'New') --acuvue i/c
            WHEN t1.advertiser_id = 'angames' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-04-28', DAY) > 90, 'Existing', 'New') --returned
            WHEN t1.advertiser_id = 'baemin_wisebirds' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-11-29', DAY) > 90, 'Existing', 'New') --baemin
            WHEN t1.advertiser_id = 'bandai_namco_new' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-03-19', DAY) > 90, 'Existing', 'New') --bandai_namco
            WHEN t1.advertiser_id = 'neowiz_jpn_new' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2019-12-21', DAY) > 90, 'Existing', 'New') --neowiz_jpn
            WHEN t1.advertiser_id = 'netmarble_japan' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-09-23', DAY) > 90, 'Existing', 'New') --returned
            WHEN t1.advertiser_id = '3q_fb' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-06-09', DAY) > 90, 'Existing', 'New') --official launch
            WHEN t1.advertiser_id = 'mytona' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-06-17', DAY) > 90, 'Existing', 'New') --U.S. winback
            WHEN t1.advertiser_id = 'ibotta' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-06-19', DAY) > 90, 'Existing', 'New') --winback
            WHEN t1.advertiser_id = 'joom' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-07-28', DAY) > 90, 'Existing', 'New') --U.S. launch
            WHEN t1.advertiser_id = 'iG1Q9PM1VAUy6tQ1' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2016-11-29', DAY) > 90, 'Existing', 'New') --woowahan bros.
            WHEN t1.advertiser_id = 'qsPeEUJGJi6tCBF9' THEN IF(DATE_DIFF(CAST(utc_date AS DATE), '2020-07-21', DAY) > 90, 'Existing', 'New') --bunjang
            WHEN first_launch IS NULL OR DATE_DIFF(CAST(utc_date AS DATE), DATE(TIMESTAMP (first_launch)), DAY) > 90 THEN 'Existing' ELSE 'New'
          END AS old_new 
       FROM(SELECT
              utc_date, 
              utc_hour,
              platform,
              advertiser_id,
              campaign_id,
              office,
              country,
              currency,
              timezone,
              CASE
                WHEN advertiser_id = 'jfW7HSSkmFZAztEA' THEN 'joyce@molocoads.com' -- Sincetimes
                ELSE sales_person
              END AS sales_person,
              account_manager,
              operation_manager,
              target_event,
              target_cost,
              kpi AS kpi_list
           FROM `focal-elf-631.df_cs_v4_campaign.tcb`
           WHERE utc_date BETWEEN table_lookback and table_str
             AND advertiser_id = advertiser_name
           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) AS t1
       LEFT JOIN (
          SELECT
            advertiser as advertiser_tcb,
            first_launch
          FROM `focal-elf-631.tcb.advertiser_first_launch_date`
          GROUP BY 1,2) AS t2
        ON t1.advertiser_id = t2.advertiser_tcb
     ) 
     , campaign_summary_all AS (
      SELECT
        DATE(ext.utc_timestamp, tcb.timezone) AS local_date,
        ext.utc_date AS utc_date,
        ext.utc_hour,
        ext.platform,
        title.advertiser,
        ext.advertiser_id,
        title.advertiser_title,
        title.campaign,
        ext.campaign_id,
        title.campaign_title,
        latest.product_name AS product,
        latest.product_display_name AS product_title,
        latest.product_category,
        latest.category_content,
        latest.tracking_bundle,
        camp_type.campaign_type AS type,
        ext.os,
        ext.country,
        ext.exchange,
        tcb.office,
        tcb.sales_person,
        tcb.account_manager,
        tcb.operation_manager,
        ext.ad_group,
        ad_group.ad_group_title,
        ext.model_name,
        ext.model_context_name,
        ext.wrapper_name,
        ext.cr_id,
        ext.cr_format,
        ext.is_vt,
        ext.is_kpi,
        ext.is_lat,
        ext.tr_group,
        ext.cr_group,
        cr_group.creative_group_title,
        ext.report_group,
        ext.report_tag,
        ext.tag_id,
        ext.format,
        tcb.target_event,
        tcb.target_cost,
        tcb.kpi_list,
        tcb.currency,
        tcb.timezone,
        tcb.old_new,
        tcb.first_launch,
        SUM(ext.moloco_spent) AS moloco_spent,
        SUM(ext.revenue) AS revenue,
        SUM(ext.revenue_adv) AS revenue_adv,
        SUM(ext.profit) AS profit,
        SUM(ext.profit_adv) AS profit_adv,
        SUM(ext.bid) AS bid,
        SUM(ext.imp) AS imp,
        SUM(ext.click) AS click,
        SUM(ext.install) AS install,
        SUM(ext.ct_install) AS ct_install,
        SUM(ext.kpi_tot) AS kpi_tot,
        SUM(ext.pb_revenue) AS pb_revenue,
        SUM(ext.pb_revenue_adv) AS pb_revenue_adv,
        SUM(total_revenue_d1) AS total_revenue_d1,
        SUM(total_revenue_d3) AS total_revenue_d3,
        SUM(total_revenue_d7) AS total_revenue_d7,
        SUM(total_revenue_d30) AS total_revenue_d30,
        SUM(count_kpi_d1) AS count_kpi_d1,
        SUM(count_kpi_d3) AS count_kpi_d3,
        SUM(count_kpi_d7) AS count_kpi_d7,
        SUM(count_kpi_d30) AS count_kpi_d30,
        SUM(count_distinct_kpi_d1) AS count_distinct_kpi_d1,
        SUM(count_distinct_kpi_d3) AS count_distinct_kpi_d3,
        SUM(count_distinct_kpi_d7) AS count_distinct_kpi_d7,
        SUM(count_distinct_kpi_d30) AS count_distinct_kpi_d30,
        SUM(total_revenue_kpi_d1) AS total_revenue_kpi_d1,
        SUM(total_revenue_kpi_d3) AS total_revenue_kpi_d3,
        SUM(total_revenue_kpi_d7) AS total_revenue_kpi_d7,
        SUM(total_revenue_kpi_d30) AS total_revenue_kpi_d30,
        SUM(ext.total_bid_price) AS total_bid_price
          FROM
            (SELECT PARSE_TIMESTAMP("%Y-%m-%d-%H", CONCAT(CAST(utc_date AS STRING),"-", utc_hour)) AS utc_timestamp,
                    utc_date,
                    utc_hour,
                    platform,
                    os,
                    country,
                    exchange,
                    ad_group,
                    model_name,
                    model_context_name, 
                    wrapper_name,
                    cr_id,
                    cr_format,
                    tr_group,
                    cr_group,
                    report_group,
                    report_tag,
                    tag_id,
                    format,
                    is_vt,
                    is_kpi,
                    is_lat,
                    advertiser_id,
                    campaign_id,
                    SUM(total_moloco_spent) AS moloco_spent,
                    SUM(total_revenue) AS revenue,
                    SUM(total_revenue_adv) AS revenue_adv,
                    SUM(total_profit) AS profit,
                    SUM(total_profit_adv) profit_adv,
                    SUM(count_bid) AS bid,
                    SUM(count_imp) AS imp,
                    SUM(count_click) AS click,
                    SUM(count_install) AS install,
                    SUM(CASE
                        WHEN is_vt IS NULL AND count_install IS NOT NULL THEN count_install
                        ELSE
                        NULL
                        END) AS ct_install,
                    SUM(count_kpi) AS kpi_tot,
                    SUM(total_pb_revenue) AS pb_revenue,
                    SUM(total_pb_revenue_adv) AS pb_revenue_adv,
                    SUM(total_revenue_d1) AS total_revenue_d1,
                    SUM(total_revenue_d3) AS total_revenue_d3,
                    SUM(total_revenue_d7) AS total_revenue_d7,
                    SUM(total_revenue_d30) AS total_revenue_d30,
                    SUM(count_kpi_d1) AS count_kpi_d1,
                    SUM(count_kpi_d3) AS count_kpi_d3,
                    SUM(count_kpi_d7) AS count_kpi_d7,
                    SUM(count_kpi_d30) AS count_kpi_d30,
                    SUM(count_distinct_kpi_d1) AS count_distinct_kpi_d1,
                    SUM(count_distinct_kpi_d3) AS count_distinct_kpi_d3,
                    SUM(count_distinct_kpi_d7) AS count_distinct_kpi_d7,
                    SUM(count_distinct_kpi_d30) AS count_distinct_kpi_d30,
                    SUM(total_revenue_kpi_d1) AS total_revenue_kpi_d1,
                    SUM(total_revenue_kpi_d3) AS total_revenue_kpi_d3,
                    SUM(total_revenue_kpi_d7) AS total_revenue_kpi_d7,
                    SUM(total_revenue_kpi_d30) AS total_revenue_kpi_d30,
                    SUM(total_bid_price) AS total_bid_price
            FROM `focal-elf-631.df_cs_v4_campaign.all_events_extended_utc` 
            WHERE utc_date BETWEEN table_lookback AND table_str
              AND advertiser_id = advertiser_name
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24 ) ext
        LEFT JOIN (
          SELECT
            utc_date, 
            utc_hour,
            platform AS platform_tcb,
            advertiser_id,
            campaign_id,
            office,
            country,
            currency,
            timezone,
            sales_person,
            account_manager,
            operation_manager,
            target_event,
            target_cost,
            kpi_list,
            old_new,
            first_launch
          FROM tcb_table
          WHERE utc_date BETWEEN table_lookback and table_str
            AND advertiser_id = advertiser_name
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
          ) AS tcb
        ON
        (ext.utc_date = tcb.utc_date
        AND ext.utc_hour = tcb.utc_hour
        AND tcb.advertiser_id = ext.advertiser_id
        AND tcb.campaign_id = ext.campaign_id
        AND tcb.platform_tcb = ext.platform)
        LEFT JOIN(
            SELECT campaign_id, campaign_type
            FROM `focal-elf-631.standard_digest.campaign_digest`
            WHERE advertiser_id = advertiser_name
            GROUP BY 1,2) AS camp_type
        ON camp_type.campaign_id = ext.campaign_id
        --AND camp_type.platform = ext.platform
        LEFT JOIN (
            SELECT creative_group_id, creative_group_title
            FROM `focal-elf-631.standard_digest.creative_group_digest`
            WHERE advertiser_id = advertiser_name
            GROUP BY 1,2) AS cr_group
        ON ext.cr_group = cr_group.creative_group_id
        --AND ext.platform = cr_group.platform
        LEFT JOIN (
            SELECT campaign_id, ad_group_id, ad_group_title
            FROM `focal-elf-631.standard_digest.ad_group_digest`
            WHERE advertiser_id = advertiser_name
            GROUP BY 1,2,3) AS ad_group
        ON ext.campaign_id = ad_group.campaign_id
        AND ext.ad_group = ad_group.ad_group_id
        --AND ext.platform = ad_group.platform
        LEFT JOIN title_lookup_table title
        ON ext.campaign_id = title.campaign_id
        -- AND ext.platform = title.platform
        LEFT JOIN (
          SELECT 
            --platform_name,
            --advertiser,
            --campaign,
            campaign_id,
            advertiser_id,
            --advertiser_title,
            --campaign_title,
            product_name,
            product_display_name,
            product_category,
            category_content,
            tracking_bundle,
            country
        FROM campaign_digest_merged_latest
        GROUP BY 1,2,3,4,5,6,7,8
        ) as latest
        ON latest.campaign_id = ext.campaign_id
        --AND latest.country = ext.country
        --AND latest.platform_name = ext.platform
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47
       )

SELECT *
FROM campaign_summary_all
) S

ON FALSE  
WHEN NOT MATCHED BY TARGET AND advertiser_id = advertiser_name AND table_lookback < DATE_ADD(local_date, INTERVAL -1 DAY) AND DATE_ADD(local_date, INTERVAL 1 DAY) <= table_str THEN INSERT ROW
WHEN NOT MATCHED BY SOURCE AND advertiser_id = advertiser_name AND table_lookback < DATE_ADD(local_date, INTERVAL -1 DAY) AND DATE_ADD(local_date, INTERVAL 1 DAY) <= table_str THEN DELETE;
------------------------------------------------------------------------------------------------------------------------
END