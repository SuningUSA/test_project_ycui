// 主表：根元素（main_report) --获得最新的信用报告，用于连接其他分表
val main_report = spark.sql("select id, report_code, create_time from fbicsi.T_BICDT_TPQR_DOCUMENT_D").
  withColumnRenamed("id","doc_id").distinct()

// 报告头表中的查询身份信息
// 身份证信息
val A = sql("select * from fbicsi.T_BICDT_TPQR_PRH_D_model").
  withColumnRenamed("id","sub_id").distinct()
val A1 = sql("select * from fbicsi.T_BICDT_TPQR_PRH_PA01_D_model").distinct()
val A2 = sql("select * from fbicsi.T_BICDT_TPQR_PRH_PA01B_D_model").
  withColumnRenamed("pa01bq01","name").
  withColumnRenamed("pa01bd01","id_type").
  withColumnRenamed("pa01bi01","id_card_num").
  withColumnRenamed("pa01bi02","query_ord_code").
  withColumnRenamed("pa01bd02","query_reason_code").
  select("prh_pa01_id",
    "name",
    "id_type",
    "id_card_num",
    "query_reason_code").distinct()

val id_card_num = A.join(A1, A.col("sub_id") === A1.col("prh_id"),"left").
  join(A2, A1.col("id") === A2.col("prh_pa01_id"),"left").
  select("doc_id",
    "id",
    "name",
    "id_type",
    "id_card_num",
    "query_reason_code").distinct()

val main_report_with_cust_id = main_report.join(id_card_num, Seq("doc_id"),"left")
main_report_with_cust_id.write.mode("overwrite").saveAsTable("usfinance.main_report_with_cust_id_yc_shichu")

//************************************************查询记录相关特征***********************************************//
// 查询记录概要表（query_summary)
val A = spark.sql("select id, doc_id from fbicsi.T_BICDT_TPQR_PQO_D_model").distinct
val A1 = spark.sql("select id, pqo_id from fbicsi.T_BICDT_TPQR_PQO_PC05_D_model").distinct
val A2 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PQO_PC05A_D_model").distinct.
withColumnRenamed("pc05aq01","last_query_reason").
withColumnRenamed("pc05ar01","last_query_date").
withColumnRenamed("pc05ad01","last_query_org").
withColumnRenamed("pc05ai01","last_query_org_code").
select("id",
"pqo_pc05_id",
"last_query_date",
"last_query_reason",
"last_query_date",
"last_query_org",
"last_query_org_code")

val A3 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PQO_PC05B_D_model").distinct.
withColumnRenamed("pc05bs01","query_loan_org_cnt_1m").
withColumnRenamed("pc05bs02","query_card_org_cnt_1m").
withColumnRenamed("pc05bs03","query_loan_cnt_1m").
withColumnRenamed("pc05bs04","query_card_cnt_1m").
withColumnRenamed("pc05bs05","query_selfquery_cnt_1m").
withColumnRenamed("pc05bs06","query_loanmanage_24m").
select("id","pqo_pc05_id","query_loan_org_cnt_1m",
"query_card_org_cnt_1m",
"query_loan_cnt_1m",
"query_card_cnt_1m",
"query_selfquery_cnt_1m",
"query_loanmanage_24m"
)

val query_summary = A.join(A1,A.col("id") === A1.col("pqo_id"),"left").
  join(A2,A1.col("id") === A2.col("pqo_pc05_id"),"left").
  join(A3,A1.col("id") === A3.col("pqo_pc05_id"),"left").
  select("doc_id",
"last_query_reason",
"last_query_date",
"last_query_org",
"last_query_org_code",
"query_loan_org_cnt_1m",
"query_card_org_cnt_1m",
"query_loan_cnt_1m",
"query_card_cnt_1m",
"query_selfquery_cnt_1m",
"query_loanmanage_24m"
).distinct()

query_summary.write.mode("overwrite").saveAsTable("usfinance.query_summary_ycui")

// 查询记录（query_record)
val A = sql("select * from fbicsi.T_BICDT_TPQR_POQ_D_model").distinct
val A1 = sql("select * from fbicsi.T_BICDT_TPQR_POQ_PH01_D_model").
  withColumnRenamed("ph010r01","query_date").
  withColumnRenamed("ph010d01","management_org_type").
  withColumnRenamed("ph010q02","query_org").
  withColumnRenamed("ph010q03","query_reason").distinct
val query_record = A.join(A1,A.col("id") === A1.col("poq_id"),"left").
  select("doc_id",
"query_date",
"management_org_type",
"query_org",
"query_reason"
).distinct()

query_record.write.mode("overwrite").saveAsTable("usfinance.query_record_ycui")


//********************************** 借贷账户信息 ****************************************** //
// 基本账户信息
val A = spark.sql("select id, doc_id from fbicsi.T_BICDT_TPQR_PDA_D_model").
  withColumnRenamed("id","sub_id").distinct()
val A1 = spark.sql("select id, pda_id from fbicsi.T_BICDT_TPQR_PDA_PD01_D_model").distinct
val A2 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PDA_PD01A_D_model").distinct.
  withColumnRenamed("pd01ai01", "account_no").
  withColumnRenamed("pd01ad01", "account_type"). // 账户类型（A.19 个人借贷账户类型代码表）
  withColumnRenamed("pd01ad03", "account_kind"). // 业务种类 -- 信用卡，借记卡，贷款等分类
  withColumnRenamed("pd01ar01", "open_date"). //开立日期
  withColumnRenamed("pd01ad04", "currency"). // 币种
  withColumnRenamed("pd01aj01", "loan_amount"). //借款金额
  withColumnRenamed("pd01aj02", "credit_limit"). //账户授信额度
  withColumnRenamed("pd01aj03", "joint_credit_limit"). //共享授信额度
  withColumnRenamed("pd01ar02", "due_date"). // 到期日期
  withColumnRenamed("pd01ad05", "payment_type"). //还款方式
  withColumnRenamed("pd01ad06", "payment_frequency"). //还款频率
  withColumnRenamed("pd01as01", "payment_term"). //还款期数
  withColumnRenamed("pd01ad07", "guarantee_type"). //担保方式
  withColumnRenamed("pd01ad08", "loan_grant_type"). //贷款发放形式
  withColumnRenamed("pd01ad09", "joint_loan_sign"). //共同借款标志
  withColumnRenamed("pd01ad10", "loan_status_at_transfer"). // 债权转移时的还款状态
select("pda_pd01_id",
  "account_no",
  "account_type",
  "account_kind",
  "open_date",
  "currency",
  "loan_amount",
  "credit_limit",
  "joint_credit_limit",
  "due_date",
  "payment_type",
  "payment_frequency",
  "payment_term",
  "guarantee_type",
  "loan_grant_type",
  "joint_loan_sign",
  "loan_status_at_transfer").distinct()

// 借贷账户最新表现信息段
val A3 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PDA_PD01B_D_model").
  withColumnRenamed("pd01bd01","account_status"). //账户状态
  withColumnRenamed("pd01br01","closed_date"). //关闭日期
  withColumnRenamed("pd01br04","transfer_month"). //转出月份
  withColumnRenamed("pd01bj01","balance"). //余额
  withColumnRenamed("pd01br02","last_payment_date"). //最近一次还款日期
  withColumnRenamed("pd01bj02","last_payment_amount"). //最近一次还款金额
  withColumnRenamed("pd01bd03","class"). //五级分类
  withColumnRenamed("pd01bd04","payment_status"). //还款状态
  withColumnRenamed("pd01br03","credit_report_date"). //信息报告日期
  select("pda_pd01_id","account_status","closed_date","transfer_month","balance",
"last_payment_date","last_payment_amount","class","payment_status","credit_report_date")

//整合
val debt_and_credit_info = A.join(A1, A.col("sub_id") === A1.col("pda_id"),"left").
  join(A2,A1.col("id") === A2.col("pda_pd01_id"),"left").
  join(A3, A1.col("id") === A3.col("pda_pd01_id"),"left").
  select("doc_id",
  "account_no",
  "account_type",
  "account_kind",
  "open_date",
  "currency",
  "loan_amount",
  "credit_limit",
  "joint_credit_limit",
  "due_date",
  "payment_type",
  "payment_frequency",
  "payment_term",
  "guarantee_type",
  "loan_grant_type",
  "joint_loan_sign",
  "loan_status_at_transfer",
  "account_status","closed_date","transfer_month","balance",
"last_payment_date","last_payment_amount","class","payment_status","credit_report_date").distinct()

debt_and_credit_info.write.mode("overwrite").saveAsTable("usfinance.debt_and_credit_info_ycui")


//********************************** 贷记卡和准贷记卡 ****************************************** //

val A = spark.sql("select * from fbicsi.T_BICDT_TPQR_PCO_D_model"). //信贷交易信息概要
  withColumnRenamed("id", "sub_id").distinct()
val A1 = spark.sql("select id, pco_id from fbicsi.T_BICDT_TPQR_PCO_PC02_D_model").distinct() // 信贷交易信息概要信息单元

// 贷记卡账户
val A2 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PCO_PC02H_D_model").
  withColumnRenamed("pc02hs01", "creditor_count_credit_card"). // 发卡机构数
  withColumnRenamed("pc02hs02", "account_count_credit_card"). //账户数
  withColumnRenamed("pc02hj01", "total_limit_credit_card"). //授信总额
  withColumnRenamed("pc02hj02", "max_limit_credit_card"). //单家行最高授信额
  withColumnRenamed("pc02hj03", "min_limit_credit_card"). //单家行最低授信额
  withColumnRenamed("pc02hj04", "balance_credit_card"). // 已用额度
  withColumnRenamed("pc02hj05", "avg_balance_6m_credit_card"). // 最近 6个月平均使用额度
  select(
  "pco_pc02_id",
  "creditor_count_credit_card",
  "account_count_credit_card",
  "total_limit_credit_card",
  "max_limit_credit_card",
  "min_limit_credit_card",
  "balance_credit_card",
  "avg_balance_6m_credit_card").distinct()

//准贷记卡账户
val A3 = spark.sql("select * from fbicsi.T_BICDT_TPQR_PCO_PC02I_D_model").
  withColumnRenamed("pc02is01", "creditor_count_secure_card"). // 发卡机构数
  withColumnRenamed("pc02is02", "account_count_secure_card"). //账户数
  withColumnRenamed("pc02ij01", "total_limit_secure_card"). //授信总额
  withColumnRenamed("pc02ij02", "max_limit_secure_card"). //单家行最高授信额
  withColumnRenamed("pc02ij03", "min_limit_secure_card"). //单家行最低授信额
  withColumnRenamed("pc02ij04", "balance_secure_card"). // 已用额度
  withColumnRenamed("pc02ij05", "avg_balance_6m_secure_card"). // 最近 6个月平均使用额度
  select(
    "pco_pc02_id",
    "creditor_count_secure_card",
    "account_count_secure_card",
    "total_limit_secure_card",
    "max_limit_secure_card",
    "min_limit_secure_card",
    "balance_secure_card",
    "avg_balance_6m_secure_card").distinct()

val credit_card_info = A.join(A1, A.col("sub_id") === A1.col("pco_id"), "left").
  join(A2, A1.col("id") === A2.col("pco_pc02_id"), "left").
  join(A3, A1.col("id") === A3.col("pco_pc02_id"),"left").
  select("doc_id",
    "creditor_count_credit_card",
    "account_count_credit_card",
    "total_limit_credit_card",
    "max_limit_credit_card",
    "min_limit_credit_card",
    "balance_credit_card",
    "avg_balance_6m_credit_card",
    "creditor_count_secure_card",
    "account_count_secure_card",
    "total_limit_secure_card",
    "max_limit_secure_card",
    "min_limit_secure_card",
    "balance_secure_card",
    "avg_balance_6m_secure_card"
  )

credit_card_info.write.mode("overwrite").saveAsTable("usfinance.credit_card_info_summary_ycui")

