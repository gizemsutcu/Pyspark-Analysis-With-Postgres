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


v_date = datetime.datetime.now()
# create temiz_prod_plan
p = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod", properties=properties)
pf = p.filter((funcs.col('st_id') != 10601) & (funcs.col('st_id') != 10604) & (funcs.col('st_id') != 10600) &
              (funcs.col('st_id') != 10605))
po = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_ofr", properties=properties)
pof = po.filter(funcs.col('is_quote_tmplt') == 1)
temiz_prod_plan = pf.select('prod_ofr_id', 'bill_acct_id', 'prod_id', 'trans_prod_id', 'st_id', 'cdate', 'udate',
                            'name', 'etl_date', 'frst_actv_date', 'prod_actv_date', 'edate') \
    .join(pof.select('prod_ofr_id', 'is_quote_tmplt'), on=['prod_ofr_id'])
temiz_prod_plan = temiz_prod_plan.withColumn('decoded_st_id', funcs.when(funcs.col('st_id') == 10606, 1)
                                             .when(funcs.col('st_id') == 10603, 2)
                                             .when(funcs.col('st_id') == 10607, 3)
                                             .when(funcs.col('st_id') == 10602, 4)
                                             .otherwise(None))
temiz_prod_plan = temiz_prod_plan.select([c for c in temiz_prod_plan.columns if c not in {'is_quote_tmplt'}])

# create birden_fazla_plani_olan_bill_acctler
result = temiz_prod_plan.join(pof.select('prod_ofr_id'), on=['prod_ofr_id'])
birden_fazla_plani_olan_bill_acctler = result.groupBy('bill_acct_id').count().filter(funcs.col('count') > 1) \
    .select('bill_acct_id')

# create bilgi_ekleme_1
bilgi_ekleme_1 = temiz_prod_plan.select('bill_acct_id', 'prod_id', 'trans_prod_id', 'decoded_st_id', 'st_id',
                                        'prod_ofr_id').join(birden_fazla_plani_olan_bill_acctler.select('bill_acct_id'),
                                                            on=['bill_acct_id'])

# create gercekten_coklayan_bill_acctler
filter = bilgi_ekleme_1.filter(funcs.col('trans_prod_id').isNull())
gercekten_coklayan_bill_acctler = filter.groupBy('bill_acct_id').count().filter(funcs.col('count') > 1) \
    .select('bill_acct_id')

# create bilgi_ekleme_2
bilgi_ekleme_2 = gercekten_coklayan_bill_acctler.select('bill_acct_id').join(temiz_prod_plan, on=['bill_acct_id'])

# create coklayip_reactive_olan_planlar_bolum_1
coklayip_reactive_olan_planlar_bolum_1 = bilgi_ekleme_2.filter(funcs.col('trans_prod_id').isNotNull())

# create coklayip_reactive_olan_planin_eski_planlarini_siralama
filter = temiz_prod_plan.filter(funcs.col('trans_prod_id').isNull())
result = filter.join(coklayip_reactive_olan_planlar_bolum_1.select('bill_acct_id'), on=['bill_acct_id'])
coklayip_reactive_olan_planin_eski_planlarini_siralama = result.withColumn("siralama", funcs.row_number().over(
    Window.partitionBy("bill_acct_id")
        .orderBy(funcs.asc("decoded_st_id"))))

# create coklayip_reactive_olan_planin_eski_planlari_bolum_2
coklayip_reactive_olan_planin_eski_planlari_bolum_2 = coklayip_reactive_olan_planin_eski_planlarini_siralama \
    .filter(funcs.col('siralama') == 1)
coklayip_reactive_olan_planin_eski_planlari_bolum_2 = coklayip_reactive_olan_planin_eski_planlari_bolum_2 \
    .select([c for c in coklayip_reactive_olan_planin_eski_planlari_bolum_2.columns if c not in {'siralama'}])

# create coklama_union_1
coklama_union_1 = coklayip_reactive_olan_planlar_bolum_1.union(coklayip_reactive_olan_planin_eski_planlari_bolum_2) \
    .distinct()

# create coklayan_reactive_olmayan_bill_acctlar
coklayan_reactive_olmayan_bill_acctlar = gercekten_coklayan_bill_acctler.select('bill_acct_id') \
    .exceptAll(coklama_union_1.select('bill_acct_id'))

# create bilgi_ekleme_3
bilgi_ekleme_3 = coklayan_reactive_olmayan_bill_acctlar.select('bill_acct_id').join(temiz_prod_plan,
                                                                                    on=['bill_acct_id'])
bilgi_ekleme_3 = bilgi_ekleme_3.withColumn('siralama', funcs.row_number().over(Window.partitionBy('bill_acct_id')
                                                                               .orderBy('decoded_st_id')))

# create coklama_union_2
coklama_union_2 = bilgi_ekleme_3.filter(funcs.col('siralama') == 1)
coklama_union_2 = coklama_union_2.select([c for c in coklama_union_2.columns if c not in {'siralama'}])

# create plan_ana_union_1
plan_ana_union_1 = coklama_union_1.union(coklama_union_2).distinct()

# create reactivated_bill_acct
reactivated_bill_acct = birden_fazla_plani_olan_bill_acctler.select('bill_acct_id') \
    .exceptAll(plan_ana_union_1.select('bill_acct_id'))

# create plan_ana_union_2
plan_ana_union_2 = reactivated_bill_acct.select('bill_acct_id').join(temiz_prod_plan, on=['bill_acct_id']) \
    .orderBy('bill_acct_id')

# create bir_bill_acct_bir_plan
bir_bill_acct_bir_plan = temiz_prod_plan.select('bill_acct_id').exceptAll(birden_fazla_plani_olan_bill_acctler
                                                                          .select('bill_acct_id'))

# create plan_ana_union_3
plan_ana_union_3 = bir_bill_acct_bir_plan.select('bill_acct_id').join(temiz_prod_plan, on=['bill_acct_id'])

# create plan_yekun
plan_ana_union_1 = plan_ana_union_1.withColumn('plan_prod_id', funcs.lit(None))
plan_ana_union_2 = plan_ana_union_2.withColumn('plan_prod_id', funcs.lit(None))
plan_ana_union_3 = plan_ana_union_3.withColumn('plan_prod_id', funcs.lit(None))
result = plan_ana_union_1.union(plan_ana_union_2).distinct()
plan_yekun = result.union(plan_ana_union_3).distinct()

# create temiz_prod_sim_activation_planli_products
po1 = po.filter((funcs.col('prod_ofr_id') != 1381) & (funcs.col('prod_ofr_id') != 9))
po1 = po1.withColumn('coalesce', funcs.coalesce(po1['is_quote_tmplt'], funcs.lit(0)))
po1f = po1.filter(funcs.col('coalesce') == 0)
pf1 = pf.filter(funcs.col('prnt_prod_id').isNotNull())
temiz_prod_sim_activation_planli_products = pf1.select('bill_acct_id', 'prod_id', 'trans_prod_id', 'st_id',
                                                       'prod_ofr_id', funcs.col('prnt_prod_id').alias('plan_prod_id'),
                                                       'cdate', 'udate', 'name', 'etl_date', 'frst_actv_date',
                                                       'prod_actv_date', 'edate') \
    .join(po1f.select('prod_ofr_id'), on=['prod_ofr_id'])
temiz_prod_sim_activation_planli_products = temiz_prod_sim_activation_planli_products.withColumn('decoded_st_id',
                                                                                                 funcs.when(funcs.col(
                                                                                                     'st_id') == 10606,
                                                                                                            1)
                                                                                                 .when(funcs.col(
                                                                                                     'st_id') == 10603,
                                                                                                       2)
                                                                                                 .when(funcs.col(
                                                                                                     'st_id') == 10607,
                                                                                                       3)
                                                                                                 .when(funcs.col(
                                                                                                     'st_id') == 10602,
                                                                                                       4)
                                                                                                 .otherwise(None))

# create temiz_prod_sim_activation_plansiz_products
pf1 = pf.filter(funcs.col('prnt_prod_id').isNull())
temiz_prod_sim_activation_plansiz_products = pf1.select('bill_acct_id', 'prod_id', 'trans_prod_id', 'st_id',
                                                        'prod_ofr_id', 'prnt_prod_id', 'cdate', 'udate', 'name',
                                                        'etl_date', 'frst_actv_date', 'prod_actv_date', 'edate') \
    .join(po1f.select('prod_ofr_id'), on=['prod_ofr_id'])
temiz_prod_sim_activation_plansiz_products = temiz_prod_sim_activation_plansiz_products.withColumn('decoded_st_id',
                                                                                                   funcs.when(funcs.col(
                                                                                                       'st_id') == 10606,
                                                                                                              1)
                                                                                                   .when(funcs.col(
                                                                                                       'st_id') == 10603,
                                                                                                         2)
                                                                                                   .when(funcs.col(
                                                                                                       'st_id') == 10607,
                                                                                                         3)
                                                                                                   .when(funcs.col(
                                                                                                       'st_id') == 10602,
                                                                                                         4)
                                                                                                   .otherwise(None))

# create plansiz_productlarin_iliskili_productlari
pr = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_rel", properties=properties)
plansiz_productlarin_iliskili_productlari = temiz_prod_sim_activation_plansiz_products.join(
    pr.select(funcs.col('prod_id1').alias('parent_prod_id'), funcs.col('prod_id2').alias('prod_id')),
    on=['prod_id'], how='left')
plansiz_productlarin_iliskili_productlari = plansiz_productlarin_iliskili_productlari \
    .select(
    [c for c in plansiz_productlarin_iliskili_productlari.columns if c not in {'decoded_st_id', 'prnt_prod_id'}])

# create plansiz_product_nihai_union_2
plansiz_product_nihai_union_2 = plansiz_productlarin_iliskili_productlari.join(temiz_prod_sim_activation_planli_products
                                                                               .select(funcs.col('prod_id')
                                                                                       .alias('parent_prod_id'),
                                                                                       'plan_prod_id'),
                                                                               on=['parent_prod_id'], how='left')

# create sim_order_product_union_3
sim_order_product_union_3 = pf.select('bill_acct_id', 'prod_id', 'trans_prod_id', 'st_id',
                                      'prod_ofr_id', funcs.col('prnt_prod_id').alias('plan_prod_id'),
                                      'cdate', 'udate', 'name', 'etl_date', 'frst_actv_date',
                                      'prod_actv_date', 'edate') \
    .join(po1.select('prod_ofr_id'), on=['prod_ofr_id'])

# create total_products
plan_yekun = plan_yekun.select([c for c in plan_yekun.columns if c not in {'decoded_st_id'}])
plansiz_product_nihai_union_2 = plansiz_product_nihai_union_2.select(
    [c for c in plansiz_product_nihai_union_2.columns if c not in {'parent_prod_id'}])
result = plan_yekun.unionByName(plansiz_product_nihai_union_2).distinct()
temiz_prod_sim_activation_planli_products = temiz_prod_sim_activation_planli_products.select(
    [c for c in temiz_prod_sim_activation_planli_products.columns if c not in {'decoded_st_id'}])
result2 = result.unionByName(temiz_prod_sim_activation_planli_products).distinct()
total_products = result2.unionByName(sim_order_product_union_3).distinct()

# create plans_will_be_added_time_span
plans_will_be_added_time_span = plan_ana_union_2.unionByName(plan_ana_union_3).distinct()

# create plans_with_time_span
plans_will_be_added_time_span = plans_will_be_added_time_span.withColumn('start_date_plan',
                                                                         funcs.when(funcs.col('trans_prod_id').isNull(),
                                                                                    funcs.col('frst_actv_date'))
                                                                         .otherwise(funcs.col('prod_actv_date')))
plans_will_be_added_time_span = plans_will_be_added_time_span.withColumn('finish_date_plan',
                                                                         funcs.lead('prod_actv_date').over(
                                                                             Window.partitionBy('bill_acct_id').orderBy(
                                                                                 'bill_acct_id', 'prod_id')))
plans_will_be_added_time_span = plans_will_be_added_time_span.withColumn('finish_date_plan', funcs.when(
    funcs.col('finish_date_plan').isNull(), datetime.datetime.now())
                                                                         .otherwise(funcs.lead('prod_actv_date').over(
    Window.partitionBy('bill_acct_id').orderBy(
        'bill_acct_id', 'prod_id'))))
plans_with_time_span = plans_will_be_added_time_span

# create products_without_plan
tp = total_products.filter((funcs.col('prod_ofr_id') != 1381) & (funcs.col('prod_ofr_id') != 9))
tpf = tp.filter(funcs.col('plan_prod_id').isNull())
po1 = po.withColumn('coalesce', funcs.coalesce(po1['is_quote_tmplt'], funcs.lit(0)))
po1f = po1.filter(funcs.col('coalesce') != 1)
products_without_plan = tpf.join(po1f.select('prod_ofr_id'), on=['prod_ofr_id'])

# create plan_added_products
products_without_plan = products_without_plan.select(
    [c for c in products_without_plan.columns if c not in {'plan_prod_id'}])
plans_with_time_span = plans_with_time_span.withColumnRenamed("bill_acct_id", "bill")
plan_added_products = products_without_plan.join(
    plans_with_time_span.select('bill', 'start_date_plan', 'finish_date_plan',
                                funcs.col('prod_id').alias('plan_prod_id')),
    (products_without_plan.bill_acct_id == plans_with_time_span.bill) & (
            products_without_plan.cdate >= plans_with_time_span.start_date_plan) & (
            products_without_plan.cdate < plans_with_time_span.finish_date_plan), how='left')
plan_added_products = plan_added_products.select([c for c in plan_added_products.columns if c not in
                                                  {'start_date_plan', 'finish_date_plan', 'bill'}])

# create total_products_with_plans
total_products = total_products.withColumnRenamed("plan_prod_id", "plan_prod")
total_products_with_plans = total_products.join(plan_added_products.select('prod_id', funcs.col('plan_prod_id')
                                                                           .alias('plan_prod_id_pap')), on=['prod_id'],
                                                how='left')
total_products_with_plans = total_products_with_plans.withColumn('plan_prod_id', funcs.when(funcs.col('plan_prod')
                                                                                            .isNotNull(),
                                                                                            funcs.col('plan_prod'))
                                                                 .otherwise(funcs.col('plan_prod_id_pap')))
total_products_with_plans = total_products_with_plans.select([c for c in total_products_with_plans.columns if c not in
                                                              {'plan_prod', 'plan_prod_id_pap'}])

# create enriched_total_products, tmp_prd_smmry_1
total_products_with_plans = total_products_with_plans.withColumn('x', funcs.coalesce(
    total_products_with_plans['plan_prod_id'], funcs.lit(0)))
enriched_total_products = total_products_with_plans.join(temiz_prod_plan.select(funcs.col('prod_id').alias('x'),
                                                                                funcs.col('prod_ofr_id')
                                                                                .alias('plan_prod_ofr_id')),
                                                         on=['x'], how='left')
enriched_total_products = enriched_total_products.select([c for c in enriched_total_products.columns if c not in {'x'}])
tmp_prd_smmry_1 = enriched_total_products  # write dwh.tmp_prd_smmry_1


# SQL-2
pol = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_ofr_lang", properties=properties)
polf = pol.filter((funcs.col('lang') == 'fr') | (funcs.col('lang') == 'en'))
polf = polf.withColumn("sira", funcs.row_number().over(Window.partitionBy("prod_ofr_id").orderBy('prod_ofr_id')))
polf = polf.withColumn('name_en', funcs.when(funcs.col('lang') == 'en', funcs.col('name'))
                       .otherwise(None))
polf = polf.withColumn('name_fr', funcs.when(funcs.col('lang') == 'fr', funcs.col('name'))
                       .otherwise(None))
polf = polf.withColumn('prod_ofr_name_en', funcs.max('name_en').over(Window.partitionBy('prod_ofr_id')))
polf = polf.withColumn('prod_ofr_name_fr', funcs.max('name_fr').over(Window.partitionBy('prod_ofr_id')))
prod_ofr_table = polf.filter(funcs.col('sira') == 1)
prod_ofr_table = prod_ofr_table.select('prod_ofr_id', 'prod_ofr_name_en', 'prod_ofr_name_fr', 'sira')

result = tmp_prd_smmry_1.join(prod_ofr_table, on=['prod_ofr_id'], how='left')
result2 = result.join(po.select('prod_ofr_id', 'is_quote_tmplt'), on=['prod_ofr_id'], how='left')
result2 = result2.withColumn('prod_name_en', funcs.coalesce(result2['prod_ofr_name_en'], result2['name']))
result2 = result2.withColumn('prod_name_fr', funcs.coalesce(result2['prod_ofr_name_fr'], result2['name']))
result2 = result2.withColumn('is_plan', funcs.coalesce(result2['is_quote_tmplt'], funcs.lit(0)))
tmp_prd_smmry_2 = result2.select([c for c in result2.columns if c not in {'name', 'sira', 'is_quote_tmplt',
                                                                          'etl_date', 'prod_ofr_name_en',
                                                                          'prod_ofr_name_fr'}])
# write dwh.tmp_prd_smmry_2

# SQL-3
st = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_gnl_st", properties=properties)
tmp_prd_smmry_3 = tmp_prd_smmry_2.join(st.select(funcs.col('gnl_st_id').alias('st_id'), funcs.col('name')
                                                 .alias('prod_status')), on=['st_id'], how='left')
# write dwh.tmp_prd_smmry_3

# SQL-4
ca = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_cust_acct", properties=properties)
caf = ca.filter(funcs.col('acct_tp_id') == 224)
tp = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_gnl_tp", properties=properties)
tpf = tp.filter(funcs.col('ent_code_name') == 'CUST_ACCT_SUB_TP')
result = tmp_prd_smmry_3.join(caf.select(funcs.col('cust_acct_id').alias('bill_acct_id'), 'acct_sub_tp_id'),
                              on=['bill_acct_id'], how='left')
tmp_prd_smmry_4 = result.join(tpf.select(funcs.col('gnl_tp_id').alias('acct_sub_tp_id'), funcs.col('shrt_code')
                                         .alias('product_category')), on=['acct_sub_tp_id'], how='left')
tmp_prd_smmry_4 = tmp_prd_smmry_4.select([c for c in tmp_prd_smmry_4.columns if c not in {'acct_sub_tp_id'}])
# write dwh.tmp_prd_smmry_4

# SQL-5
ps = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_srvc", properties=properties)
s = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_srvc", properties=properties)
prod_srvc = ps.select('srvc_id', funcs.col('prod_id').alias('prod_id_ps')).join(s.select('srvc_id', 'srvc_spec_id'),
                                                                                on=['srvc_id'])

pr = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_prod_rsrc", properties=properties)
r = spark.read.jdbc(url=url, table="dwh_stg.stg_dce_rsrc", properties=properties)
prod_rsrc = pr.select('rsrc_id', funcs.col('prod_id').alias('prod_id_pr')).join(r.select('rsrc_id', 'rsrc_spec_id'),
                                                                                on=['rsrc_id'])

sum_of_specs = prod_srvc.select('prod_id_ps', 'srvc_spec_id').join(prod_rsrc.select('prod_id_pr', 'rsrc_spec_id'),
                                                                   prod_srvc.prod_id_ps == prod_rsrc.prod_id_pr,
                                                                   how='full')
sum_of_specs = sum_of_specs.withColumn('prod_id',
                                       funcs.when(funcs.col('prod_id_ps').isNotNull(), funcs.col('prod_id_ps'))
                                       .otherwise(funcs.col('prod_id_pr')))
sum_of_specs = sum_of_specs.select([c for c in sum_of_specs.columns if c not in {'prod_id_ps', 'prod_id_pr'}])

tmp_prd_smmry_5 = tmp_prd_smmry_4.join(sum_of_specs, on=['prod_id'], how='left')
# write dwh.tmp_prd_smmry_5

# SQL-6 (insert into dwt_pre_prod_summary)
dwt_pre_prod_summary = spark.read.jdbc(url=url, table="dwh.dwt_pre_prod_summary", properties=properties)
tmp_prd_smmry_5 = tmp_prd_smmry_5.withColumnRenamed('st_id', 'prod_st_id').withColumnRenamed('udate', 'prod_udate') \
    .withColumnRenamed('cdate', 'prod_cdate')
tmp_prd_smmry_5 = tmp_prd_smmry_5.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
dwt_pre_prod_summary = dwt_pre_prod_summary.unionByName(tmp_prd_smmry_5)
# write dwh.dwt_pre_prod_summary

# SQL-7 (update dwt_prod_summary)
dwt_prod_summary = spark.read.jdbc(url=url, table="dwh.dwt_prod_summary", properties=properties)
dwt_pre_prod_summary = dwt_pre_prod_summary.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
equal = dwt_pre_prod_summary.join(dwt_prod_summary, on=['prod_id'], how='left_semi')
not_equal = dwt_prod_summary.join(dwt_pre_prod_summary, on=['prod_id'], how='left_anti')
dwt_prod_summary = equal.unionByName(not_equal)
# write dwh.dwt_prod_summary

# SQL-8 (insert into dwt_prod_summary)
dwt_pre_prod_summary = dwt_pre_prod_summary.withColumn('etl_date', funcs.lit(datetime.datetime.now()))
insert = dwt_pre_prod_summary.join(dwt_prod_summary, on=['prod_id'], how='left_anti')
dwt_prod_summary = dwt_prod_summary.unionByName(insert)
# write dwh.dwt_prod_summary

endTime = datetime.datetime.now()
print("BASLAMA ZAMANI: ", v_date)
print("BITIS ZAMANI: ", endTime)
print("KOD TOTAL ZAMAN: ", endTime - v_date)
