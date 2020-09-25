from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as funcs
import datetime
from pyspark.sql.functions import rank

# author: GİZEM SÜTÇÜ

url = "jdbc:postgresql://ip:port/dbName"
properties = {"user": "userName", "password": "password", "driver": "org.postgresql.Driver"}

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("postgres-pyspark") \
    .config("spark.jars", "C:\\path_to_postgresDriver\\postgresql-42.2.16.jar") \
    .getOrCreate()

v_date = datetime.datetime.now()
# SQL-1
df_a = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_party", properties=properties)
df_b = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust", properties=properties)
df_gst = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_gnl_st", properties=properties)
df_ctp = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_tp", properties=properties)
df_gtp = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_gnl_tp", properties=properties)
result = df_b.select('cust_id', 'party_id', funcs.col('st_id').alias('st_id_cust'), 'cust_tp_id', 'new_cust_id',
                     'cust_since') \
    .join(
    df_a.select('party_id', 'party_tp_id', funcs.col('st_id').alias('st_id_party'), 'frst_name', 'mname', 'lst_name',
                'nick_name',
                'edu_id', 'brth_date', 'brth_plc', 'gendr_id', 'mrtl_st_id', 'mthr_name', 'fthr_name',
                'scl_secr_num', 'occp_id', 'incm_lvl_id', 'nat_id', 'org_name', 'org_tp_id', 'tax_id',
                'tax_ofc', funcs.col('sdate').alias('sdate_party'), funcs.col('edate').alias('edate_party'),
                funcs.col('cdate').alias('cdate_party'), funcs.col('cuser').alias('cuser_party'),
                funcs.col('udate').alias('udate_party'),
                funcs.col('uuser').alias('uuser_party'), 'email', 'mobile_phone', 'fax', 'home_phone', 'facebook_url',
                'linkedin_url',
                'wrk_phone', 'refer_code'), on=['party_id'], how='left')
result1 = result.join(df_gst.select('gnl_st_id', funcs.col('name').alias('st')), result.st_id_cust == df_gst.gnl_st_id,
                      how='inner')
result2 = result1.join(df_ctp.select('cust_tp_id', funcs.col('name').alias('cust_tp')), on=['cust_tp_id'], how='inner')
result_sql1 = result2.join(df_gtp.select('gnl_tp_id', funcs.col('name').alias('party_tp')),
                           result2.party_tp_id == df_gtp.gnl_tp_id,
                           how='inner')
result_sql1 = result_sql1.select([c for c in result_sql1.columns if c not in {'gnl_st_id', 'gnl_tp_id'}])

# SQL-2
df = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_acct", properties=properties)
cust_acct = df.groupBy('cust_id').count().select(funcs.col("cust_id"), funcs.col("count").alias("cust_acct_count"))
result_sql2 = result_sql1.join(cust_acct, on=['cust_id'], how='left')

# SQL-3
ccca = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_credit_card_cust_acct", properties=properties)
df1 = ccca.join(df, on=['cust_acct_id'], how='inner')
credit_card_count = df1.groupBy('cust_id').count().select(funcs.col("cust_id"),
                                                          funcs.col("count").alias("credit_card_count"))
result_sql3 = result_sql2.join(credit_card_count, on=['cust_id'], how='left')

# SQL-4
df = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_addr", properties=properties)
lpm = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_lylty_prg_memb", properties=properties)
addr = df.withColumn("rank", rank().over(Window.partitionBy("row_id").orderBy(funcs.desc("addr_id")))) \
    .select('city_id', 'city_name', 'cntry_id', 'cntry_name', 'row_id', 'rank')
addr2 = addr.filter(addr.rank == 1.0)
result = result_sql3.join(addr2, result_sql3.cust_id == addr2.row_id, how='left')
lpm = lpm.select(funcs.col('cust_id').alias('cust_id_lpm'))
result_sql4 = result.join(lpm.select('cust_id_lpm'), result.cust_id == lpm.cust_id_lpm, how='left')
result_sql4 = result_sql4.withColumn('is_prg_memb', funcs.when(funcs.col('cust_id_lpm').isNotNull(), 1).otherwise(0))
result_sql4 = result_sql4.drop('cust_id_lpm')

# SQL-5
cacq = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_acq", properties=properties)
cust_acq = cacq.withColumn("rank", rank().over(Window.partitionBy("cust_id").orderBy(funcs.desc("cust_acq_id")))) \
    .select('cust_id', 'web_acq_source', 'web_acq_medium', 'web_acq_campaign', 'cdate', 'rank', 'cust_acq_id')
cust_acq2 = cust_acq.filter(cust_acq.rank == 1.0).sort(funcs.desc("cdate"))
result_sql5 = result_sql4.join(cust_acq2.select('cust_id', 'web_acq_source', 'web_acq_medium', 'web_acq_campaign'),
                               on=['cust_id'], how='left')

# SQL-6
dwf = spark.read.jdbc(url=url, table="dwh.dwf_gift_detail", properties=properties)
gd = dwf.filter(funcs.col('trgt_cust_id').isNotNull()).select('src_cust_id').distinct()
result_sql6 = result_sql5.join(gd, result_sql5.cust_id == gd.src_cust_id, how='left')
result_sql6 = result_sql6.withColumn('is_gift', funcs.when(funcs.col('src_cust_id').isNotNull(), 1).otherwise(0))
result_sql6 = result_sql6.drop('src_cust_id')

# SQL-7
stg = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_refer_invit_hstr", properties=properties)
referral_detail = stg.filter((funcs.col('st_id') == 10751) &
                             (str(funcs.col('src_alt_val')) != funcs.col('trgt_alt_val'))) \
    .select('src_cust_id').distinct()
result_sql7 = result_sql6.join(referral_detail, result_sql6.cust_id == referral_detail.src_cust_id, how='left')
result_sql7 = result_sql7.withColumn('is_referral', funcs.when(funcs.col('src_cust_id').isNotNull(), 1).otherwise(0))
result_sql7 = result_sql7.drop('src_cust_id')

# SQL-8
ccp = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_cmmnc_pref", properties=properties)
ccp = ccp.filter(funcs.col('is_actv') == 1).select('cust_id', 'ntf_topic_id', 'is_slct').distinct()
ccp = ccp.withColumn('is_marketing', funcs.when(funcs.col('ntf_topic_id') == 10000, ccp.select("is_slct")
                                                .rdd.max()[0]).otherwise(0))
ccp = ccp.withColumn('is_referral_t', funcs.when(funcs.col('ntf_topic_id') == 30000, 1).otherwise(0))
ccp = ccp.withColumn('is_cc_expire', funcs.when(funcs.col('ntf_topic_id') == 70000, 1).otherwise(0))
ccp = ccp.withColumn('is_usage_75', funcs.when(funcs.col('ntf_topic_id') == 110000, 1).otherwise(0))
ccp = ccp.withColumn('is_usage_90', funcs.when(funcs.col('ntf_topic_id') == 110001, 1).otherwise(0))
ccp = ccp.withColumn('is_usage_100', funcs.when(funcs.col('ntf_topic_id') == 110002, 1).otherwise(0))
ccp = ccp.withColumn('is_transaction_confirmation', funcs.when(funcs.col('ntf_topic_id') == 50000, 1).otherwise(0))
ccp = ccp.withColumn('is_roaming_zone_change', funcs.when(funcs.col('ntf_topic_id') == 90000, 1).otherwise(0))
ccp = ccp.withColumn('is_fair_data', funcs.when(funcs.col('ntf_topic_id') == 40000, 1).otherwise(0))
result_sql8 = ccp.select('cust_id', 'is_marketing', 'is_referral_t', 'is_cc_expire', 'is_usage_75', 'is_usage_90',
                         'is_usage_100', 'is_transaction_confirmation', 'is_roaming_zone_change', 'is_fair_data')

# SQL-9
try:
    pref = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_syst_cmmnc_pref", properties=properties)
    syst_cmmnc_pref = pref.filter(pref.is_actv == 1)
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_marketing_default', funcs.when(funcs.col('ntf_topic_id') == 10000,
                                                                                    syst_cmmnc_pref.select(
                                                                                        "is_slct").rdd.max()[
                                                                                        0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_referral_t_default', funcs.when(funcs.col('ntf_topic_id') == 30000,
                                                                                     syst_cmmnc_pref.select(
                                                                                         "is_slct").rdd.max()[
                                                                                         0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_cc_expire_default', funcs.when(funcs.col('ntf_topic_id') == 70000,
                                                                                    syst_cmmnc_pref.select(
                                                                                        "is_slct").rdd.max()[
                                                                                        0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_usage_75_default', funcs.when(funcs.col('ntf_topic_id') == 110000,
                                                                                   syst_cmmnc_pref.select(
                                                                                       "is_slct").rdd.max()[
                                                                                       0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_usage_90_default', funcs.when(funcs.col('ntf_topic_id') == 110001,
                                                                                   syst_cmmnc_pref.select(
                                                                                       "is_slct").rdd.max()[
                                                                                       0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_usage_100_default', funcs.when(funcs.col('ntf_topic_id') == 110002,
                                                                                    syst_cmmnc_pref.select(
                                                                                        "is_slct").rdd.max()[
                                                                                        0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_transaction_confirmation_default', funcs.when(
        funcs.col('ntf_topic_id') == 50000, syst_cmmnc_pref.select("is_slct").rdd.max()[0]).otherwise(0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_roaming_zone_change_default', funcs.when(
        funcs.col('ntf_topic_id') == 90000, syst_cmmnc_pref.select("is_slct").rdd.max()[0]).otherwise(0))
    syst_cmmnc_pref = syst_cmmnc_pref.withColumn('is_fair_data_default', funcs.when(funcs.col('ntf_topic_id') == 40000,
                                                                                    syst_cmmnc_pref.select(
                                                                                        "is_slct").rdd.max()[
                                                                                        0]).otherwise(
        0))
    syst_cmmnc_pref = syst_cmmnc_pref.select('is_marketing_default', 'is_referral_t_default', 'is_cc_expire_default',
                                             'is_usage_75_default', 'is_usage_90_default', 'is_usage_100_default',
                                             'is_transaction_confirmation_default', 'is_roaming_zone_change_default',
                                             'is_fair_data_default')
    result = result_sql7.join(result_sql8, on=['cust_id'], how='left')
    result_sql9 = result.join(syst_cmmnc_pref, 1 == 1, how='left')
    result_sql9 = result_sql9.withColumn('is_marketing', funcs.when(funcs.col('is_marketing').isNotNull(),
                                                                    funcs.col('is_marketing'))
                                         .otherwise(funcs.col('is_marketing_default')))
    result_sql9 = result_sql9.withColumn('is_referral_t', funcs.when(funcs.col('is_referral_t').isNotNull(),
                                                                     funcs.col('is_referral_t'))
                                         .otherwise(funcs.col('is_referral_t_default')))
    result_sql9 = result_sql9.withColumn('is_cc_expire', funcs.when(funcs.col('is_cc_expire').isNotNull(),
                                                                    funcs.col('is_cc_expire'))
                                         .otherwise(funcs.col('is_cc_expire_default')))
    result_sql9 = result_sql9.withColumn('is_usage_75', funcs.when(funcs.col('is_usage_75').isNotNull(),
                                                                   funcs.col('is_usage_75'))
                                         .otherwise(funcs.col('is_usage_75_default')))
    result_sql9 = result_sql9.withColumn('is_usage_90', funcs.when(funcs.col('is_usage_90').isNotNull(),
                                                                   funcs.col('is_usage_90'))
                                         .otherwise(funcs.col('is_usage_90_default')))
    result_sql9 = result_sql9.withColumn('is_usage_100', funcs.when(funcs.col('is_usage_100').isNotNull(),
                                                                    funcs.col('is_usage_100'))
                                         .otherwise(funcs.col('is_usage_100_default')))
    result_sql9 = result_sql9.withColumn('is_transaction_confirmation',
                                         funcs.when(funcs.col('is_transaction_confirmation')
                                                    .isNotNull(),
                                                    funcs.col('is_transaction_confirmation'))
                                         .otherwise(funcs.col('is_transaction_confirmation_default')))
    result_sql9 = result_sql9.withColumn('is_roaming_zone_change', funcs.when(funcs.col('is_roaming_zone_change')
                                                                              .isNotNull(),
                                                                              funcs.col('is_roaming_zone_change'))
                                         .otherwise(funcs.col('is_roaming_zone_change_default')))
    result_sql9 = result_sql9.withColumn('is_fair_data', funcs.when(funcs.col('is_fair_data').isNotNull(),
                                                                    funcs.col('is_fair_data'))
                                         .otherwise(funcs.col('is_fair_data_default')))
except:
    result_sql9 = result_sql7.join(result_sql8, on=['cust_id'], how='left')

# SQL-10
au = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_apl_user", properties=properties)
ll = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_lang", properties=properties)
lang = au.select(funcs.col('party_id').alias('prty_id'), 'pref_lang_id', 'ntf_pref_lang_id').join(ll.select('lang_id',
                                                                                                            funcs.col(
                                                                                                                'name').alias(
                                                                                                                'pref_lang')),
                                                                                                  au.pref_lang_id == ll.lang_id,
                                                                                                  how='left')
lang = lang.drop('lang_id')
lang2 = lang.join(ll.select('lang_id', funcs.col('name').alias('ntf_pref_lang')),
                  lang.ntf_pref_lang_id == ll.lang_id, how='left')
lang2 = lang2.drop('lang_id')
result_sql = result_sql9.join(lang2, result_sql9.party_id == lang2.prty_id, how='left')
result_sql10 = result_sql.join(au.select('party_id', 'st_id'), on=['party_id'], how='left')
result_sql10 = result_sql10.withColumn('invalid_email', funcs.when((174 <= funcs.col('st_id')) &
                                                                   (funcs.col('st_id') <= 178), 1).otherwise(0))
result_sql10 = result_sql10.drop('st_id')

# SQL-11
cst = spark.read.jdbc(url=url, table="dwh.dwd_pre_customer", properties=properties)
cst = cst.select([c for c in cst.columns if c not in {'cust_acq_id', 'is_actv_cust_acq'}])
tmp10 = result_sql10.select([c for c in result_sql10.columns if c not in {'prty_id'}])
result_sql11 = cst.unionByName(tmp10)

# SQL-12
dwd = spark.read.jdbc(url=url, table="dwh.dwd_customer", properties=properties)
not_equal = dwd.join(result_sql11, on=['cust_id'], how='left_anti')
equal = result_sql11.join(dwd, on=['cust_id'], how='left_semi')
equal = equal.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
not_equal = not_equal.select([c for c in not_equal.columns if c not in {'cust_acq_id', 'is_actv_cust_acq'}])
result_sql12 = equal.unionByName(not_equal)

# second way (long-code)
# dwd = spark.read.jdbc(url=url, table="dwh.dwd_customer", properties=properties)
# etl_date = datetime.datetime.now()
# dnm = result_sql11.withColumn("equal", funcs.lit(0))
# result = dwd.join(dnm.select('cust_id', 'equal'), on=['cust_id'], how='left')
# not_equal = result.filter(funcs.col('equal').isNull())
# not_equal = not_equal.drop('equal')
# update_dwd = dwd.exceptAll(not_equal)
# dnm = dnm.drop('equal')
# result1 = dnm.join(update_dwd, on=['cust_id'], how='left_semi')
# result1 = result1.withColumn("etl_date", funcs.lit(etl_date))
# not_equal = not_equal.select([c for c in not_equal.columns if c not in {'cust_acq_id', 'is_actv_cust_acq'}])
# result_sql12 = result1.unionByName(not_equal)

# SQL-13
append = result_sql11.join(result_sql12, on=['cust_id'], how='left_anti')
append = append.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
result_sql13 = result_sql12.unionByName(append)

# second way (long-code)
# result = result_sql11.select('cust_id').join(result_sql12, on=['cust_id'], how='left')
# d = result.withColumn('exists', funcs.when(funcs.col('etl_date').isNotNull(), 1).otherwise(0))
# d = d.filter(d.exists == 0)
# etl_date = datetime.datetime.now()
# d = d.withColumn("etl_date", funcs.lit(etl_date))
# d = d.drop('exists')
# result_sql13 = result_sql12.unionByName(d)

# SQL-14
dwd_hstr1 = spark.read.jdbc(url=url, table="dwh.dwd_hstr_customer", properties=properties)
dwd_hstr = dwd_hstr1.filter(dwd_hstr1.is_current_record == 1)
dwd_hstr2 = dwd_hstr.select([c for c in dwd_hstr.columns if c not in {'effective_from_date', 'is_current_record',
                                                                      'effective_to_date', 'sys_effective_from_date',
                                                                      'sys_effective_to_date', 'cust_acq_id',
                                                                      'is_actv_cust_acq'}])
result_sql14 = result_sql13.exceptAll(dwd_hstr2)  # _11 table

# SQL-15
result_sql15 = dwd_hstr.join(result_sql11.select('cust_id', funcs.col('udate_party').alias('udate_party_11')),
                             on=['cust_id'])
result_sql15 = result_sql15.withColumn("effective_to_date", funcs.lit(funcs.col('udate_party_11')))
result_sql15 = result_sql15.drop('udate_party_11')
result_sql15 = result_sql15.withColumn("is_current_record", funcs.lit(0))
result_sql15 = result_sql15.withColumn("sys_effective_to_date", funcs.lit(v_date))  # dww_hstr

# SQL-16
result_sql14 = result_sql14.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
result_sql14 = result_sql14.withColumn('effective_from_date', funcs.when(funcs.col('udate_party').isNotNull(),
                                                                         funcs.col('udate_party'))
                                       .otherwise(funcs.col('cdate_party')))
result_sql14 = result_sql14.withColumn("effective_to_date", funcs.to_timestamp(funcs.lit(None)))
result_sql14 = result_sql14.withColumn("is_current_record", funcs.lit(1))
result_sql14 = result_sql14.withColumn("sys_effective_to_date", funcs.to_timestamp(funcs.lit(None)))
result_sql14 = result_sql14.withColumn("sys_effective_from_date", funcs.lit(v_date))
dwd_hstr = result_sql15.select([c for c in dwd_hstr.columns if c not in {'cust_acq_id', 'is_actv_cust_acq'}])
result_sql14 = result_sql14.select([c for c in result_sql14.columns if c not in {'cust_acq_id', 'is_actv_cust_acq'}])
result_sql16 = dwd_hstr.unionByName(result_sql14)

endTime = datetime.datetime.now()
print("BASLAMA ZAMANI: ", v_date)
print("BITIS ZAMANI: ", endTime)
print("KOD TOTAL ZAMAN: ", endTime - v_date)
