#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import sys
import time
import datetime
import logging

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


ONE_DAY = datetime.timedelta(days=1)


class Logger(object):
    """docstring for Logger"""
    def __init__(self, path, clevel=logging.DEBUG, flevel=logging.DEBUG):
        super(Logger, self).__init__()
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        # 设置CMD日志
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(clevel)

        # 设置文件日志
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        fh.setLevel(flevel)

        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def war(self, msg):
        self.logger.warn(msg)

    def error(self, msg):
        self.logger.error(msg)

    def cri(self, msg):
        self.logger.critical(msg)
        


def exec_time_utils(start_exec_time):
    last_time = time.time() - start_exec_time
    m, s = divmod(last_time, 60)
    h, m = divmod(m, 60)
    return "Elapse Times: %02d:%02d:%02d" % (h, m, s)


def get_before_day_str(olddate, format="%Y%m%d", days=1):
    if isinstance(olddate, str):
        olddate = datetime.datetime.strptime(olddate, format)
    elif isinstance(olddate, datetime.datetime):
        before_date = olddate - datetime.timedelta(days=days)
        return before_date.strftime("%Y%m%d")


def buid_query_sql(data_date):
    '''
    build indices and get sql
    --------
    data_date: str
        eg: '20180801',通过时间按日拼接sql ，获取日分区数据
    return
    --------
    cookie_dau_goods_rela_sql: str
        cookie_dau_goods_relation_event 中间表指标查询sql

    goods_detail_duration_sql: str
        ods_page_view_log 日志表获取 goods detail 访问时长查询sql

    goods_paid_order_sql: str
        cookie_order 中间表获取下单次数、购买次数查询sql

    goods_click_gd_event_sql: str
        ods_event_log 事件日志表获取 goods_click 和其它 goods detail页面事件指标查询sql

    '''
    # 获取中间表指标 cookie_dau_goods_relation_event
    cookie_dau_goods_rela_sql = "\
        select\
            cookie_id as cookieid\
            ,goods_id as goodsid\
            ,site_id as siteid\
            ,visit_cnt as visit_cnt\
            ,cart_cnt as cart_cnt\
            ,fav_cnt as fav_cnt\
            ,share_cnt as share_cnt\
            ,checkout_cnt as checkout_cnt\
            ,pay_type_cnt as pay_type_cnt\
            ,signed_cnt as signed_cnt\
            ,reg_cnt as reg_cnt\
            ,trytopay_cnt as trytopay_cnt\
            ,0 as gd_visit_duration\
            ,0 as paid_cnt\
            ,0 as order_cnt\
            ,0 as goods_impression_cnt\
            ,0 as goods_click_cnt\
            ,0 as gd_reviews_click_cnt\
            ,0 as gd_reviews_more_click_cnt\
            ,0 as gd_details_click_cnt\
            ,0 as gd_select_click_cnt\
            ,0 as gd_share_click_cnt\
            ,0 as gd_shopgoods_click_cnt\
            ,0 as cart_click_cnt\
            ,0 as cart_submit_click_cnt\
            ,0 as fav_recommend_cnt\
            ,0 as fav_main_cnt\
            ,0 as sizeguide_click_cnt\
            ,0 as gd_shop_tips_click_cnt\
            ,0 as gd_brand_click_cnt\
            ,0 as gd_brand_recommend_cnt\
            ,0 as gd_coupon_click_cnt\
            ,0 as gd_coupon_receive_cnt\
        from dw.cookie_dau_goods_relation_event\
        where data_date = '"+ data_date +"'\
            and site_id not in(400, 700)\
            and goods_id > 0\
            and regexp_replace(cookie_id, '[a-zA-Z0-9\-]', '') = ''\
    "

    # pageview 表获取详情页访问时长 ods_page_view_log
    goods_detail_duration_sql = "\
        select\
            a.cookieid as cookieid\
            ,b.gid as goodsid\
            ,a.siteid as siteid\
            ,0 as visit_cnt\
            ,0 as cart_cnt\
            ,0 as fav_cnt\
            ,0 as share_cnt\
            ,0 as checkout_cnt\
            ,0 as pay_type_cnt\
            ,0 as signed_cnt\
            ,0 as reg_cnt\
            ,0 as trytopay_cnt\
            ,sum(a.pageview_duration) as gd_visit_duration\
            ,0 as paid_cnt\
            ,0 as order_cnt\
            ,0 as goods_impression_cnt\
            ,0 as goods_click_cnt\
            ,0 as gd_reviews_click_cnt\
            ,0 as gd_reviews_more_click_cnt\
            ,0 as gd_details_click_cnt\
            ,0 as gd_select_click_cnt\
            ,0 as gd_share_click_cnt\
            ,0 as gd_shopgoods_click_cnt\
            ,0 as cart_click_cnt\
            ,0 as cart_submit_click_cnt\
            ,0 as fav_recommend_cnt\
            ,0 as fav_main_cnt\
            ,0 as sizeguide_click_cnt\
            ,0 as gd_shop_tips_click_cnt\
            ,0 as gd_brand_click_cnt\
            ,0 as gd_brand_recommend_cnt\
            ,0 as gd_coupon_click_cnt\
            ,0 as gd_coupon_receive_cnt\
        from\
        (\
            select cookieid\
              ,siteid\
              ,get_json_object(segment, '$.pvid2') as pvid2\
              ,max(pageview_duration) as pageview_duration\
            from ods.ods_page_view_log\
            where data_date = '"+ data_date +"'\
              and pagename = 'GoodsDetail'\
              and siteid not in(300, 400)\
              and regexp_replace(cookieid, '[a-zA-Z0-9\-]', '') = ''\
              and get_json_object(segment, '$.pvid2') is not null\
              group by cookieid, siteid, get_json_object(segment, '$.pvid2')\
        ) a\
        inner join\
        (\
            select\
                cookieid\
                ,siteid\
                ,pvid2\
                ,gid\
            from\
            (\
                select cookieid\
                  ,siteid\
                  ,get_json_object(segment, '$.pvid2') as pvid2\
                  ,get_json_object(segment, '$.gid') as gid\
                  ,row_number() over(partition by cookieid, siteid, get_json_object(segment, '$.pvid2') order by servertime desc) as desc_no\
                from ods.ods_page_view_log\
                where data_date = '"+ data_date +"'\
                  and pagename = 'GoodsDetail'\
                  and siteid not in(400, 700)\
                  and regexp_replace(cookieid, '[a-zA-Z0-9\-]', '') = ''\
                  and get_json_object(segment, '$.pvid2') is not null\
                  and get_json_object(segment, '$.gid') is not null\
            ) p\
            where desc_no = 1\
        ) b\
        on a.cookieid = b.cookieid and a.pvid2 = b.pvid2 and a.siteid = b.siteid\
        where b.gid > 0\
        group by a.cookieid, b.gid, a.siteid\
    "

    # 中间表获取下单和购买指标 cookie_order
    goods_paid_order_sql = "\
        select\
            cookieid as cookieid\
            ,goodsid as goodsid\
            ,site_id as siteid\
            ,0 as visit_cnt\
            ,0 as cart_cnt\
            ,0 as fav_cnt\
            ,0 as share_cnt\
            ,0 as checkout_cnt\
            ,0 as pay_type_cnt\
            ,0 as signed_cnt\
            ,0 as reg_cnt\
            ,0 as trytopay_cnt\
            ,0 as gd_visit_duration\
            ,sum(is_paid) as paid_cnt\
            ,sum(is_order) as order_cnt\
            ,0 as goods_impression_cnt\
            ,0 as goods_click_cnt\
            ,0 as gd_reviews_click_cnt\
            ,0 as gd_reviews_more_click_cnt\
            ,0 as gd_details_click_cnt\
            ,0 as gd_select_click_cnt\
            ,0 as gd_share_click_cnt\
            ,0 as gd_shopgoods_click_cnt\
            ,0 as cart_click_cnt\
            ,0 as cart_submit_click_cnt\
            ,0 as fav_recommend_cnt\
            ,0 as fav_main_cnt\
            ,0 as sizeguide_click_cnt\
            ,0 as gd_shop_tips_click_cnt\
            ,0 as gd_brand_click_cnt\
            ,0 as gd_brand_recommend_cnt\
            ,0 as gd_coupon_click_cnt\
            ,0 as gd_coupon_receive_cnt\
        from dw.cookie_order\
        where data_date ='"+ data_date +"'\
            and site_id not in(400, 700)\
            and goodsid > 0\
            and regexp_replace(cookieid, '[a-zA-Z0-9\-]', '') = ''\
        group by cookieid\
            ,goodsid\
            ,site_id\
    "

    # 中间表获取goods 曝光指标 dw_cookie_dau_goods_relation_imp
    goods_imp_sql = "\
        select\
            cookie_id as cookieid\
            ,goods_id as goodsid\
            ,site_id as siteid\
            ,0 as visit_cnt\
            ,0 as cart_cnt\
            ,0 as fav_cnt\
            ,0 as share_cnt\
            ,0 as checkout_cnt\
            ,0 as pay_type_cnt\
            ,0 as signed_cnt\
            ,0 as reg_cnt\
            ,0 as trytopay_cnt\
            ,0 as gd_visit_duration\
            ,0 as paid_cnt\
            ,0 as order_cnt\
            ,imp_cnt as goods_impression_cnt\
            ,0 as goods_click_cnt\
            ,0 as gd_reviews_click_cnt\
            ,0 as gd_reviews_more_click_cnt\
            ,0 as gd_details_click_cnt\
            ,0 as gd_select_click_cnt\
            ,0 as gd_share_click_cnt\
            ,0 as gd_shopgoods_click_cnt\
            ,0 as cart_click_cnt\
            ,0 as cart_submit_click_cnt\
            ,0 as fav_recommend_cnt\
            ,0 as fav_main_cnt\
            ,0 as sizeguide_click_cnt\
            ,0 as gd_shop_tips_click_cnt\
            ,0 as gd_brand_click_cnt\
            ,0 as gd_brand_recommend_cnt\
            ,0 as gd_coupon_click_cnt\
            ,0 as gd_coupon_receive_cnt\
        from dw.dw_cookie_dau_goods_relation_imp\
        where data_date = '"+ data_date +"'\
            and site_id not in(400, 700)\
            and goods_id > 0\
            and regexp_replace(cookie_id, '[a-zA-Z0-9\-]', '') = ''\
    "

    # 事件日志表获取 goods_click 和其它详情页事件指标 ods_event_log
    goods_click_gd_event_sql = "\
        select\
            cookieid\
            ,goodsid\
            ,siteid\
            ,0 as visit_cnt\
            ,0 as cart_cnt\
            ,0 as fav_cnt\
            ,0 as share_cnt\
            ,0 as checkout_cnt\
            ,0 as pay_type_cnt\
            ,0 as signed_cnt\
            ,0 as reg_cnt\
            ,0 as trytopay_cnt\
            ,0 as gd_visit_duration\
            ,0 as paid_cnt\
            ,0 as order_cnt\
            ,0 as goods_impression_cnt\
            ,sum(goods_click) as goods_click_cnt\
            ,sum(gd_reviews_click) as gd_reviews_click_cnt\
            ,sum(gd_reviews_more_click) as gd_reviews_more_click_cnt\
            ,sum(gd_details_click) as gd_details_click_cnt\
            ,sum(gd_select_click) as gd_select_click_cnt\
            ,sum(gd_share_click) as gd_share_click_cnt\
            ,sum(gd_shopgoods_click) as gd_shopgoods_click_cnt\
            ,sum(cart_click) as cart_click_cnt\
            ,sum(cart_submit_click) as cart_submit_click_cnt\
            ,sum(fav_recommend) as fav_recommend_cnt\
            ,sum(fav_main) as fav_main_cnt\
            ,sum(sizeguide_click) as sizeguide_click_cnt\
            ,sum(gd_shopping_tips_click) as gd_shop_tips_click_cnt\
            ,sum(gd_brand_click) as gd_brand_click_cnt\
            ,sum(gd_brandgoods_click) as gd_brand_recommend_cnt\
            ,sum(gd_coupon_click) as gd_coupon_click_cnt\
            ,sum(gd_coupon_receive) as gd_coupon_receive_cnt\
        from\
        (\
            select\
                cookieid\
                ,goodsid\
                ,siteid\
                ,case when lower(eventkey)='goods_click'                                      then 1 else 0 end as goods_click\
                ,case when lower(eventkey)='goodsdetail_reviews_click'                        then 1 else 0 end as gd_reviews_click\
                ,case when lower(eventkey)='goodsdetail_reviews_more_click'                   then 1 else 0 end as gd_reviews_more_click\
                ,case when lower(eventkey)='goodsdetail_details_click'                        then 1 else 0 end as gd_details_click\
                ,case when lower(eventkey)='goodsdetail_select_click'                         then 1 else 0 end as gd_select_click\
                ,case when lower(eventkey)='goodsdetail_share_click'                          then 1 else 0 end as gd_share_click\
                ,case when lower(eventkey)='goodsdetail_shopgoods_click'                      then 1 else 0 end as gd_shopgoods_click\
                ,case when lower(eventkey)='addtobag_click'                                   then 1 else 0 end as cart_click\
                ,case when lower(eventkey)='addtobag_submit_click'                            then 1 else 0 end as cart_submit_click\
                ,case when lower(eventkey)='wishlist_add' and lable='1'                       then 1 else 0 end as fav_recommend\
                ,case when lower(eventkey)='wishlist_add' and lable='1'                       then 1 else 0 end as fav_main\
                ,case when lower(eventkey)='addtobag_sizeguide_click'                         then 1 else 0 end as sizeguide_click\
                ,case when lower(eventkey)='goodsdetail_shopping_tips_click'                  then 1 else 0 end as gd_shopping_tips_click\
                ,case when lower(eventkey)='goodsdetail_brand_click'                          then 1 else 0 end as gd_brand_click\
                ,case when lower(eventkey)='goodsdetail_brandgoods_click'                     then 1 else 0 end as gd_brandgoods_click\
                ,case when lower(eventkey)='goodsdetail_coupon_click'                         then 1 else 0 end as gd_coupon_click\
                ,case when lower(eventkey)='goodsdetail_coupon_receive_result' and result='1' then 1 else 0 end as gd_coupon_receive\
            from ods.ods_event_log\
            where data_date = '"+ data_date +"'\
                and siteid not in(400, 700)\
                and goodsid > 0\
                and regexp_replace(cookieid, '[a-zA-Z0-9\-]', '') = ''\
                and lower(eventkey) in(\
                    'goods_click'\
                    ,'goodsdetail_reviews_click'\
                    ,'goodsdetail_reviews_more_click'\
                    ,'goodsdetail_details_click'\
                    ,'goodsdetail_select_click'\
                    ,'goodsdetail_share_click'\
                    ,'goodsdetail_shopgoods_click'\
                    ,'addtobag_click'\
                    ,'addtobag_submit_click'\
                    ,'wishlist_add'\
                    ,'addtobag_sizeguide_click' \
                    ,'goodsdetail_shopping_tips_click' \
                    ,'goodsdetail_brand_click' \
                    ,'goodsdetail_brandgoods_click' \
                    ,'goodsdetail_coupon_click' \
                    ,'goodsdetail_coupon_receive_result')\
        ) p\
        group by cookieid\
            ,goodsid\
            ,siteid\
    "

    return cookie_dau_goods_rela_sql, goods_detail_duration_sql, goods_paid_order_sql, goods_imp_sql, goods_click_gd_event_sql


def main():
    if 1 == len(sys.argv):
        today = datetime.datetime.today()
        yesterday_str = get_before_day_str(today)
        start_date_in = yesterday_str
        end_date_in = start_date_in
    elif 2 == len(sys.argv):
        start_date_in = sys.argv[1]
        end_date_in = start_date_in
    elif 3 == len(sys.argv):
        start_date_in = sys.argv[1]
        end_date_in = sys.argv[2]
    else:
        print "Illegal Parameters!"
        return
    
        # 标记开始加工时间，统计耗时
    start_time = time.time()
    start_date_str = str(start_date_in)
    end_date_str = str(end_date_in)

    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")

    date_timedelta = end_date - start_date
    epochs = date_timedelta.days+1
    #logger.info('time range：'+start_date_str+'to'+end_date_str+' cycle：'+str(epochs))

    # 循环处理
    spark = SparkSession.builder.appName('userprofile_features_build').enableHiveSupport().getOrCreate()
    partition_date_str = start_date_str
    partition_date = start_date
    for epoch in range(epochs):
        epoch_start_time = time.time()
		# 传入日期参数,返回待执行的str
        cookie_dau_goods_rela_sql, goods_detail_duration_sql, goods_paid_order_sql, goods_imp_sql, goods_click_gd_event_sql = buid_query_sql(partition_date_str)

        df_cookie_dau_goods_rela = spark.sql(cookie_dau_goods_rela_sql)
        df_goods_detail_duration = spark.sql(goods_detail_duration_sql)
        df_goods_paid_order_duration = spark.sql(goods_paid_order_sql)
        df_goods_imp = spark.sql(goods_imp_sql)
        df_goods_click_gd_event = spark.sql(goods_click_gd_event_sql)

		# 将用户各维度的行为特征union all起来
        df_cookie_goods_features = df_cookie_dau_goods_rela.unionAll(df_goods_detail_duration).unionAll(df_goods_paid_order_duration).unionAll(df_goods_imp).unionAll(df_goods_click_gd_event)
        df_cookie_goods_features_sum = df_cookie_goods_features.groupBy('cookieid', 'goodsid', 'siteid').sum(*df_cookie_goods_features.columns[3:])
        
		# 创建临时视图
		df_cookie_goods_features_sum.createOrReplaceTempView('tmp_view_df_cookie_goods_features_sum')
        # 插入目标分区表中
		insert_sql = "\
            insert overwrite table dw.dw_cookie_goods_log_day partition(data_date='"+ partition_date_str +"')\
            select * from tmp_view_df_cookie_goods_features_sum\
        "
        spark.sql(insert_sql)
        partition_elapse = exec_time_utils(epoch_start_time)
        #logger.info(partition_date_str+'daily data finished，'+partition_elapse)
        
        # 处理循环变量
        partition_date = datetime.datetime.strptime(partition_date_str, "%Y%m%d")+ONE_DAY
        partition_date_str = partition_date.strftime("%Y%m%d")
    all_elapse = exec_time_utils(start_time)

if __name__ == '__main__':
    main()
