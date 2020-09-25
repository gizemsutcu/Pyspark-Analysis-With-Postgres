from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as funcs
import datetime

# author: GİZEM SÜTÇÜ

url = "jdbc:postgresql://ip:port/dbName"
properties = {"user": "userName", "password": "password", "driver": "org.postgresql.Driver"}

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("postgres-pyspark") \
    .config("spark.jars", "C:\\path_to_postgresDriver\\postgresql-42.2.16.jar") \
    .getOrCreate()

# mode = "overwrite"
v_date = datetime.datetime.now()
# SQL-1
co = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_ord", properties=properties)
coi = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_ord_item", properties=properties)
bi = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_bsn_inter", properties=properties)
b1 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_ord_item_actn_tp", properties=properties)
b2 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_bsn_flow_spec", properties=properties)
b3 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_bsn_inter_spec", properties=properties)
b4 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_bsn_inter_st", properties=properties)
b5 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_ord_item_st", properties=properties)
b6 = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_ord_st", properties=properties)
result1 = co.select('cust_ord_id', 'ord_st_id', 'bsn_flow_spec_id', 'sale_cnl_id', 'item_cnt').join(
    coi.select('cust_ord_id', 'bsn_inter_id', 'cust_ord_item_id', 'ord_item_st_id', 'new_cust_bill_acct_id',
               'cust_bill_acct_id', 'cust_acct_id', 'new_cust_acct_id', 'prod_id', 'prod_ofr_id', 'cust_ord_item_tp_id',
               funcs.col('prod_spec_id').alias('coi_prod_spec_id'), 'cust_id', 'new_cust_id', 'ord_trans_tp_id',
               'ord_item_actn_tp_id', 'ofr_name', 'prod_num', funcs.col('cdate').alias('coi_cdate'),
               funcs.col('cuser').alias('coi_cuser'), funcs.col('udate').alias('coi_udate'),
               funcs.col('uuser').alias('coi_uuser'), 'actvt_date', 'inst_addr_id', 'new_inst_addr_id', 'bill_addr_id',
               'new_bill_addr_id', 'qty', 'bs_prc_init', 'bs_prc_prd', 'bs_prc_usg', 'tax_bs_prc_init',
               'tax_bs_prc_prd', 'tax_bs_prc_usg', 'req_dlv_date', 'actl_dlv_date', 'is_need_shpmt', 'dlv_st',
               'dlv_trk_link', 'dlv_trk_num', 'is_slct', 'prod_name'), on=['cust_ord_id'], how='inner')
result2 = result1.join(bi.select('bsn_inter_id', 'bsn_inter_spec_id', 'bsn_inter_st_id', 'bsn_inter_rsn_tp_id',
                                 funcs.col('sale_cnl_id').alias('bi_sale_cnl_id')), on=['bsn_inter_id'], how='inner')
result3 = result2.filter((funcs.col('bsn_inter_spec_id') == 2) | (funcs.col('bsn_inter_spec_id') == 5230) |
                         (funcs.col('bsn_inter_spec_id') == 5150))
result3 = result3.withColumn('dlv_st', funcs.when(funcs.col('dlv_st').isNotNull(), funcs.col('dlv_st'))
                             .otherwise('Order Creation'))
result4 = result3.join(b1.select('ord_item_actn_tp_id', funcs.col('name').alias('ord_item_actn_tp_name'),
                                 funcs.col('shrt_code').alias('ord_item_actn_tp_shrt_code')),
                       on=['ord_item_actn_tp_id'])
result5 = result4.join(b2.select('bsn_flow_spec_id', funcs.col('shrt_code').alias('bsn_flow_spec')),
                       on=['bsn_flow_spec_id'])
result6 = result5.join(b3.select('bsn_inter_spec_id', funcs.col('name').alias('bsn_inter_spec')),
                       on=['bsn_inter_spec_id'])
result7 = result6.join(b4.select('bsn_inter_st_id', funcs.col('name').alias('bsn_inter_st')), on=['bsn_inter_st_id'])
result8 = result7.join(b5.select('ord_item_st_id', funcs.col('name').alias('ord_item_st')), on=['ord_item_st_id'])
result_sql1 = result8.join(b6.select('ord_st_id', funcs.col('name').alias('ord_st')), on=['ord_st_id'])
# write table _sales1
# elde edilen analiz sonuclarının database e tablo şeklinde kaydedilme islemi
# result_sql1.write.jdbc(url=url, table="pyspark_sql1", mode=mode, properties=properties)

# SQL-2
# read _sales1 table ---> result_sql1
po = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_ofr", properties=properties)
# _sales1 tablosu ile stg_dce_prod_ofr tablosu, prod_ofr_id alanına göre birleştirilip _sales2 tablosu oluşturuldu
result_sql2 = result_sql1.join(po.select('prod_ofr_id', 'prod_spec_id', 'prod_ofr_tp_id', 'is_bndl',
                                         funcs.col('st_id').alias('prod_ofr_st_id')), on=['prod_ofr_id'], how='left')
# write table _sales2

# SQL-3
# read _sales2 table ---> result_sql2
ps = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_spec", properties=properties)
# _sales2 tablosu ile stg_dce_prod_spec tablosu, prod_spec_id alanına göre birleştirilip _sales3 tablosu oluşturuldu
result_sql3 = result_sql2.join(ps.select('prod_spec_id', 'is_bill', 'is_chrg', 'is_inst_rqrd', 'is_shpmt_rqrd',
                                         funcs.col('st_id').alias('prod_spec_st_id')), on=['prod_spec_id'], how='left')
# write table _sales3

# SQL-4
# read _sales3 table ---> result_sql3
b = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_spec_srvc_spec", properties=properties)
b = b.filter(funcs.col('is_actv') == 1)
# _sales3 tablosu ile filtrelenen(is_actv==1) stg_dce_prod_spec_srvc_spec tablosu, prod_spec_id alanına göre
# birleştirilip _sales4 oluşturuldu
result_sql4 = result_sql3.join(b.select('prod_spec_id', 'srvc_spec_id'), on=['prod_spec_id'], how='left')
# write table _sales4

# SQL-5
# read _sales4 table ---> result_sql4
b = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_srvc_spec", properties=properties)
# _sales4 tablosu ile stg_dce_srvc_spec tablosu, srvc_spec_id alanına göre birleştirilip _sales5 tablosu oluşturuldu
result_sql5 = result_sql4.join(b.select('srvc_spec_id', funcs.col('name').alias('srvc_spec_name')), on=['srvc_spec_id'],
                               how='left')
# write table _sales5

# SQL-6
# read _sales5 table ---> result_sql5
c = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust", properties=properties)
# _sales5 tablosu ile stg_dce_cust tablosu, cust_id alanına göre birleştirilip _sales6 tablosu oluşturuldu
result_sql6 = result_sql5.join(c.select('cust_id', 'party_role_id', 'party_id', 'cust_tp_id'), on=['cust_id'],
                               how='left')
# write table _sales6

# SQL-7
# read _sales6 table ---> result_sql6
p = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_ord_char_val", properties=properties)
# stg_dce_cust_ord_char_val tablosunda is_actv==1 ve char_id==420 olan kayıtlar filtrelendi
pf = p.filter((funcs.col('is_actv') == 1) & (funcs.col('char_id') == 420))
rank_filter = pf.withColumn("rank", funcs.rank().over(Window.partitionBy("cust_ord_id")
                                                      .orderBy(funcs.desc("cust_ord_char_val_id")))) \
    .select('cust_ord_id', 'char_id', 'val', 'rank')
# filtrelenen kayıtlar üzerinden cust_ord_id alanına göre rank metodu ile gruplanıp, cust_ord_char_val_id alanına göre
# azalan şekilde sıralanan veriler rank değeri 1 olanlar için _co_chval tablosu oluşturuldu
co_chval = rank_filter.filter(funcs.col('rank') == 1)
# write table _co_chval

# SQL-8
# read _sales6 table ---> result_sql6
# read _co_chval table ---> co_chval
# _sales6 tablosu ile _co_chval tablosu, cust_ord_id alanına göre birleştirilip _sales7 tablosu oluşturuldu
result_sql7 = result_sql6.join(co_chval.select('cust_ord_id', funcs.col('char_id').alias('gnl_char_id'),
                                               funcs.col('val').alias('refer_val')), on=['cust_ord_id'], how='left')
# write table _sales7

# SQL-9
# read _sales7 table ---> result_sql7
cv = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_gnl_char", properties=properties)
# _sales7 tablosunun gnl_char_id alanı ile stg_dce_gnl_char tablosunun char_id alanına göre veriler birleştirilip
# _sales8 tablosu oluşturuldu
result_sql8 = result_sql7.join(cv.select(funcs.col('char_id').alias('gnl_char_id'),
                                         funcs.col('shrt_code').alias('refer_shrt_code')),
                               on=['gnl_char_id'], how='left')
# write table _sales8

# SQL-10
cov = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_ord_item_char_val", properties=properties)
# stg_dce_cust_ord_item_char_val tablosunun char_id==527 ve is_actv==1 şartına uyan verileri ile _sales9 oluşturuldu.
result_sql9 = cov.filter((funcs.col('char_id') == 527) & (funcs.col('is_actv') == 1)) \
    .select('cust_ord_item_id', funcs.col('val').alias('mnp_provider'))
# write table _sales9

# SQL-11
# read stg_dce_cust_ord_char_val table ---> p from sql-7
# stg_dce_cust_ord_char_val tablosunda is_actv==1 ve char_id==10016 olan kayıtlar filtrelendi
pf = p.filter((funcs.col('is_actv') == 1) & (funcs.col('char_id') == 10016))
pf = pf.withColumn('x', funcs.coalesce(pf['udate'], pf['cdate']))
# filtrelenen kayıtlar üzerinden cust_ord_id alanına göre rank metodu ile gruplanıp, udate(eğer udate NULL ise cdate)
# alanına göre veriler sıralandı (coalesce metodu ilk NULL olmayan alanın verisini döndürür)
rank_filter = pf.withColumn("rank", funcs.rank().over(Window.partitionBy("cust_ord_id")
                                                      .orderBy("x"))) \
    .select('cust_ord_id', 'val', 'rank')
# tablodaki val değerinin içeriğine göre bb_provider alanı olusturuldu
rank_filter = rank_filter.withColumn('bb_provider', funcs.when(funcs.col('val') != '""',
                                                               funcs.col('val')).otherwise(None))
rank_filter = rank_filter.drop('val')   # val alanı bbp tablosunda bulunması gerekli olmadığından silindi
bbp = rank_filter.filter(funcs.col('rank') == 1)   # rank değeri 1 olan kayıtlar için _bbp tablosu oluşturuldu
# write table _bbp

# SQL-12
# read stg_dce_cust_ord_item_char_val table ---> cov from sql-10
# char_id==591 ve is_actv==1 şartını sağlayan veriler için _sales10 oluşturuldu ve val alanı mobile_provider olarak
# adlandırıldı
result_sql10 = cov.filter((funcs.col('char_id') == 591) & (funcs.col('is_actv') == 1)).select('cust_ord_item_id',
                                                                                              funcs.col('val')
                                                                                              .alias('mobile_provider'))
# write table _sales10

# SQL-13
# read stg_dce_cust_ord_item_char_val table ---> cov from sql-10
# char_id==63 ve is_actv==1 şartını sağlayan veriler için _sales11 oluşturuldu ve val alanı mnp_new_provider olarak
# adlandırıldı
result_sql11 = cov.filter((funcs.col('char_id') == 63) & (funcs.col('is_actv') == 1)).select('cust_ord_item_id',
                                                                                             funcs.col('val')
                                                                                             .alias('mnp_new_provider'))
# write table _sales11

# SQL-14
# read _sales8 table ---> result_sql8
# read _sales9 table ---> result_sql9
# _sales8 ile _sales9 tabloları cust_ord_item_id alanına göre birleştirildi ve _sales12 oluşturuldu
result_sql12 = result_sql8.join(result_sql9.select('cust_ord_item_id', 'mnp_provider'), on=['cust_ord_item_id'],
                                how='left')
# write table _sales12

# SQL-15
# read _sales12 table ---> result_sql12
# read _sales10 table ---> result_sql10
# _sales12 ile _sales10 tabloları cust_ord_item_id alanına göre birleştirildi ve _sales13 oluşturuldu
result_sql13 = result_sql12.join(result_sql10.select('cust_ord_item_id', 'mobile_provider'), on=['cust_ord_item_id'],
                                 how='left')
# write table _sales13

# SQL-16
# read _sales13 table ---> result_sql13
# read _sales11 table ---> result_sql11
# _sales13 ile _sales11 tabloları cust_ord_item_id alanına göre birleştirildi ve _sales14 oluşturuldu
result_sql14 = result_sql13.join(result_sql11.select('cust_ord_item_id', 'mnp_new_provider'), on=['cust_ord_item_id'],
                                 how='left')
# write table _sales14

# SQL-17
# read stg_dce_cust_ord_char_val table ---> p from sql-7
# stg_dce_cust_ord_char_val tablosunda is_actv==1 ve char_id==526 olan kayıtlar filtrelendi
pf = p.filter((funcs.col('is_actv') == 1) & (funcs.col('char_id') == 526))
rank_filter = pf.withColumn("rank", funcs.rank().over(Window.partitionBy("cust_ord_id")
                                                      .orderBy(funcs.desc('cust_ord_char_val_id')))) \
    .select('cust_ord_id', 'char_id', funcs.col('val').alias('is_mnp'), 'rank')
# filtrelenen kayıtlar üzerinden cust_ord_id alanına göre rank metodu ile gruplanıp, cust_ord_char_val_id alanına göre
# azalan şekilde sıralanan veriler rank değeri 1 olanlar için _is_mnp tablosu oluşturuldu
is_mnp = rank_filter.filter(funcs.col('rank') == 1)
# write table _is_mnp

# SQL-18
# read _sales14 table ---> result_sql14
p1 = spark.read.jdbc(url=url, table="dwh.dwt_prod_summary", properties=properties)
# _sales14 ile dwt_prod_summary tabloları prod_id alanlarına göre birleştirildi ve _sales17 oluşturuldu
result_sql17 = result_sql14.join(
    p1.select('prod_id', 'product_category', funcs.col('plan_prod_id').alias('prnt_prod_id')), on=['prod_id'],
    how='left')
# write table _sales17

# SQL-19
# read _sales17 table ---> result_sql17
hstr = spark.read.jdbc(url=url, table="dwh.dwt_msisdn_change_hstr", properties=properties)
# dwt_msisdn_change_hstr tablosunda is_current_record==1 olan veriler filtrelendi
hstr1 = hstr.filter(funcs.col('is_current_record') == 1)
result_sql17 = result_sql17.withColumn('x', funcs.coalesce(result_sql17['prnt_prod_id'], result_sql17['prod_id']))
# _sales17 tablosunun cust_bill_acct_id alanı ile filtrelenen dwt_msisdn_change_hstr tablosunun bill_acct_id alanı,
# _sales17 de prnt_prod_id alanı(NULL ise prod_id) ile dwt_msisdn_change_hstr tablosunun prod_id alanlarına göre
# tablolar birleştirildi, sonucunda _sales18 oluşturuldu
result = result_sql17.join(hstr1.select(funcs.col('bill_acct_id').alias('cust_bill_acct_id'),
                                        funcs.col('prod_id').alias('x'), 'msisdn_number'),
                           on=['cust_bill_acct_id', 'x'], how='left')
# oluşturulan _sales18 tablosundan coalesce işlemi için oluşturulan(geçici) alan çıkarıldı
result_sql18 = result.select([c for c in result.columns if c not in {'x'}])
# write table _sales18

# SQL-20
# read _sales18 table ---> result_sql18
# read _bbp table ---> bbp from sql-11
result_sql19 = result_sql18.join(bbp.select('cust_ord_id', 'bb_provider'), on=['cust_ord_id'], how='left')
result_sql19 = result_sql19.withColumn('provider', funcs.when(
    (funcs.col('mobile_provider').isNull()) & (funcs.col('product_category') == 'BROADBAND'), funcs.col('bb_provider'))
                                       .otherwise(funcs.col('mobile_provider')))
# _sales18 ile _bbp tabloları cust_ord_id alana göre birleştirilip, mobile_provider alanının NULL olup
# product_category alan değerinin 'BROADBAND' olduğu kayıtların yeni oluşturulan provider alanı için bb_provider
# alanının aktarılması, bu koşulu sağlamayan kayıtlar içinde provider alanına mobile_provider alan değerinin
# aktarılması sağlandı ve _sales19 da gerekli olmayan bb_provider alanı çıkarılarak _sales19 oluşturuldu
result_sql19 = result_sql19.select([c for c in result_sql19.columns if c not in {'bb_provider'}])
# write table _sales19

# SQL-21
# read _sales19 table ---> result_sql19
# read _is_mnp table ---> is_mnp from result_sql17
result_sql20 = result_sql19.join(is_mnp.select('cust_ord_id', funcs.col('is_mnp').alias('_is_mnp')), on=['cust_ord_id'],
                                 how='left')
result_sql20 = result_sql20.withColumn('is_mnp', funcs.when(
    (funcs.col('provider').isNotNull()) & (funcs.col('product_category') == 'BROADBAND'), 1)
                                       .otherwise(funcs.col('_is_mnp')))
# _sales19 ile _is_mnp tabloları cust_ord_id alana göre birleştirilip, provider alanının NULL olmayıp
# product_category alan değerinin 'BROADBAND' olduğu kayıtların yeni oluşturulan is_mnp alanı için 1
# değerinin aktarılması, bu koşulu sağlamayan kayıtlar içinde provider alanına is_mnp tablosundaki is_mnp alan değerinin
# aktarılması sağlandı ve _sales20 da gerekli olmayan _is_mnp alanı çıkarılarak _sales20 oluşturuldu
result_sql20 = result_sql20.select([c for c in result_sql20.columns if c not in {'_is_mnp'}])
# write table _sales20

# SQL-22
# read _sales14 table ---> result_sql14
prod = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod", properties=properties)
addr = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_addr", properties=properties)
# stg_dce_addr tablosunda condo_unit_id!=NULL, addr_tp_id==1 ve data_tp_id==20 şartını sağlayan veriler filtrelendi
adr = addr.filter((funcs.col('condo_unit_id').isNotNull()) & (funcs.col('addr_tp_id') == 1) &
                  (funcs.col('data_tp_id') == 20))
result_sql21 = prod.select('prod_id').join(adr.select(funcs.col('row_id').alias('prod_id'), 'condo_unit_id', 'udate'),
                                           on=['prod_id'], how='left')
# stg_dce_prod tablosu ile filtrelenen stg_dce_addr tablosu prod_id alanına göre birleştirildi ve prod_id,
# condo_unit_id ve udate alanları gruplanarak _sales21 oluşturuldu
result_sql21 = result_sql21.select('prod_id', 'condo_unit_id', 'udate').distinct()
# write table _sales21

# SQL-23
# read _sales21 table ---> result_sql21
result_sql21 = result_sql21.withColumn("rn", funcs.rank().over(Window.partitionBy("prod_id")
                                                               .orderBy(funcs.desc("udate")))) \
    .select('prod_id', 'condo_unit_id', 'udate', 'rn')
sales21_rn = result_sql21.filter(funcs.col('rn') == 1)
# _sales21 tablosu rank metodu ile prod_id alanını gruplayıp udate alanı ile azalan şekilde sıralanmasıyla rank
# değeri 1 olan kayıtlar için _sales21_rn oluşturuldu
# write table _sales21_rn

# SQL-24
# read _sales20 table ---> result_sql20
# read _sales21 table ---> result_sql21
# _sales20 ile _sales21 tabloları prod_id alanlarına göre birleştirildi ve _sales 22 oluşturuldu
result_sql22 = result_sql20.join(result_sql21.select('prod_id', 'condo_unit_id'), on=['prod_id'], how='left')
# write table _sales22

# SQL-25
# read _sales22 table ---> result_sql22
prc = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_prc", properties=properties)
pt = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prc_chrg_tp", properties=properties)
# stg_dce_prod_prc tablosunda prc_chrg_tp_id alanı 1 ya da 2 olan kayıtlar filtrelendi
prf = prc.filter((funcs.col('prc_chrg_tp_id') == 1) | (funcs.col('prc_chrg_tp_id') == 2))
result = result_sql22.join(prf.select('prod_id', funcs.col('calc_prod_ofr_prc_val').alias('activation_prc1'),
                                      'prc_chrg_tp_id'), on=['prod_id'], how='left')
result_sql23 = result.join(pt.select('prc_chrg_tp_id', funcs.col('name').alias('prc_chrg_tp_name')),
                           on=['prc_chrg_tp_id'])
# _sales22 ile filtrelenen stg_dce_prod_prc tablosu prod_id alanlarına göre birleştirilip, sonucunda
# stg_dce_prc_chrg_tp tablosu ile prc_chrg_tp_id alanına göre birleştirildi ve _sales23 için prc_chrg_tp_id çıkarıldı
result_sql23 = result_sql23.select([c for c in result_sql23.columns if c not in {'prc_chrg_tp_id'}])
# write table _sales23

# SQL-26
# read _sales23 table ---> result_sql23
result_sql23 = result_sql23.withColumn('coalesce', funcs.coalesce(result_sql23['activation_prc1'], funcs.lit(0)))
planprc = result_sql23.select(funcs.col('prnt_prod_id').alias('prod_id'), 'coalesce') \
    .groupBy('prod_id').sum('coalesce').select('prod_id', funcs.col("sum(coalesce)").alias("plan_prc"))
result_sql23 = result_sql23.drop('coalesce')
# _sales23 tablosunda prod_id alanı gruplanıp activation_prc1(NULL ise 0 olarak al) toplandı, toplam değeri
# plan_plan_prc olarak adlandırıldı ve _planprc oluşturuldu
# write table _planprc

# SQL-27
# read _sales23 table ---> result_sql23
# read _planprc table ---> planprc from sql-26
result_sql24 = result_sql23.join(planprc, on=['prod_id'], how='left')
result_sql24 = result_sql24.withColumn('activation_prc',
                                       funcs.when((funcs.col('activation_prc1').isNull()) & (funcs.col('is_bndl') == 1),
                                                  funcs.col('plan_prc'))
                                       .otherwise(funcs.col('activation_prc1')))
# _sales23 ile planprc tabloları prod_id alanına göre birleştirildi, activation_prc1=NULL ve is_bndl=1 koşulunu
# sağlayan kayıtlar için yeni oluşturulan activation_prc alanına plan_prc alanının verisini aktarırken koşulu
# sağlamayan kayıtlar için activation_prc1 alanının verisi aktarıldı  ve _sales24 için plan_prc çıkarıldı
result_sql24 = result_sql24.select([c for c in result_sql24.columns if c not in {'plan_prc'}])
# write table _sales24

# SQL-28
# read _sales24 table ---> result_sql24
dwf = spark.read.jdbc(url=url, table="dwh.dwf_pre_sales", properties=properties)
result_sql24 = result_sql24.withColumn("etl_date", funcs.lit(datetime.datetime.now()))
dwf = dwf.select([c for c in dwf.columns if c not in {'ident_val1', 'co_cdate', 'co_cuser', 'co_udate', 'co_uuser',
                                                      'co_sdate', 'co_edate'}])
result_sql24 = result_sql24.select([c for c in result_sql24.columns if c not in {'mobile_provider', 'activation_prc1'}])
# unionByName metodu ile dwf_pre_sales tablosuna _sales24 tablosunun verileri eklendi
dwf_pre_sales = dwf.unionByName(result_sql24)
# write table dwf_pre_sales

# SQL-29
# read dwf_pre_sales table ---> dwf_pre_sales
dwf = spark.read.jdbc(url=url, table="dwh.dwf_sales", properties=properties)
# left_anti joinleme türü ile dwf_sales tablosunun cust_ord_item_id alanına göre dwf_pre_sales deki verilerle
# uyuşmayan verileri alındı
not_equal = dwf.join(dwf_pre_sales, on=['cust_ord_item_id'], how='left_anti')
# left_semi joinleme türü ile dwf_pre_sales tablosunun cust_ord_item_id alanına göre dwf_sales deki verilerle uyuşan
# verileri alındı
equal = dwf_pre_sales.join(dwf, on=['cust_ord_item_id'], how='left_semi')
equal = equal.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
not_equal = not_equal.select(
    [c for c in not_equal.columns if c not in {'ident_val1', 'co_cdate', 'co_cuser', 'co_udate',
                                               'co_uuser', 'co_sdate', 'co_edate'}])
# left_semi ile left_anti metodu sonuçları unionByName ile birleştirilerek, dwf_sales in cust_ord_item_id alanına göre
# dwf_pre_sales e göre uyuşan verileri güncellenmiş oldu
dwf_sales = equal.unionByName(not_equal)
# write table dwf_sales

# SQL-30
# read dwf_pre_sales table ---> dwf_pre_sales
result = dwf_pre_sales.join(dwf_sales, on=['cust_ord_item_id'], how='left_anti')
result = result.withColumn("etl_date", funcs.lit(datetime.datetime.now()))
# dwf_pre_sales tablosunun cust_ord_item_id alana göre dwf_sales te var olmayan kayıtlar left_anti ile belirlendi ve
# unionByName ile dwf_sales e insert edildi
dwf_sales = dwf_sales.unionByName(result)
# write table dwf_sales

# SQL-31
# read dwf_sales table ---> dwf_sales
sale = spark.read.jdbc(url=url, table="dwh.dwf_hstr_sales", properties=properties)
salef = sale.filter(funcs.col('is_current_record') == 1)
dwf_sales = dwf_sales.select([c for c in dwf_sales.columns if c not in {'etl_date'}])
salef = salef.select([c for c in salef.columns if c not in {'co_cdate', 'co_cuser', 'co_udate', 'co_uuser', 'co_sdate',
                                                            'co_edate', 'effective_from_date', 'is_current_record',
                                                            'effective_to_date', 'sys_effective_from_date',
                                                            'sys_effective_to_date'}])
# exceptAll metodu ile dwf_sales tablosunda, filtrelenen dwf_hstr_sales tablosundaki veriler var ise except edildi ve
# _hstr oluşturuldu
hstr = dwf_sales.exceptAll(salef)
# write table _hstr

# SQL-32
# read dwf_hstr_sales table ---> sale from sql-31 'is_current_record=1 --> salef from sql-31'
# read _hstr table ---> hstr from sql-31
dwf_hstr_sales = salef.join(hstr.select('cust_ord_item_id', funcs.col('coi_udate').alias('coi_udate_hs')),
                            on=['cust_ord_item_id'])
dwf_hstr_sales = dwf_hstr_sales.withColumn("effective_to_date", funcs.lit(funcs.col('coi_udate_hs')))
dwf_hstr_sales = dwf_hstr_sales.withColumn("is_current_record", funcs.lit(0))
dwf_hstr_sales = dwf_hstr_sales.withColumn("sys_effective_to_date", funcs.lit(v_date))
dwf_hstr_sales = dwf_hstr_sales.drop('coi_udate_hs')
# filtrelenen dwf_hstr_sales tablosu ile _hstr tablosunun cust_ord_item_id alanı ile uyuşan verileri için
# dwf_hstr_sales tablosunun lit() metodu ile bazı alanları güncellendi
# write table dwf_hstr_sales

# SQL-33
# read dwf_hstr_sales table ---> dwf_hstr_sales from sql-32
# read _hstr table ---> hstr from sql-31
dwf_hstr_sales = dwf_hstr_sales.withColumn('effective_from_date', funcs.lit(None))
dwf_hstr_sales = dwf_hstr_sales.withColumn('sys_effective_from_date', funcs.lit(None))
hstr = hstr.withColumn('effective_from_date', funcs.when(funcs.col('coi_udate').isNotNull(), funcs.col('coi_udate'))
                       .otherwise(funcs.col('coi_udate')))
hstr = hstr.withColumn('is_current_record', funcs.lit(1))
hstr = hstr.withColumn('effective_to_date', funcs.lit(None))
hstr = hstr.withColumn('sys_effective_from_date', funcs.lit(v_date))
hstr = hstr.withColumn('sys_effective_to_date', funcs.lit(None))
# dwf_hstr_sales tablosuna unionByName ile _hstr tablosundaki verilerin bir kısmı güncellenip insert edildi
dwf_hstr_sales = dwf_hstr_sales.unionByName(hstr)
# write table dwf_hstr_sales

endTime = datetime.datetime.now()
print("BASLAMA ZAMANI: ", v_date)
print("BITIS ZAMANI: ", endTime)
print("KOD TOTAL ZAMAN: ", endTime - v_date)
