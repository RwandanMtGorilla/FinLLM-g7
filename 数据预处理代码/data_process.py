import pandas as pd
from tqdm import tqdm


# 读取四个Excel文件
df1 = pd.read_excel("big_data1_processed.xlsx", engine='openpyxl')
df2 = pd.read_excel("big_data2_processed.xlsx", engine='openpyxl')
df3 = pd.read_excel("big_data3.xlsx", engine='openpyxl')
df4 = pd.read_excel('industry_.xlsx', engine='openpyxl')

# 检查“文件名”列是否存在
if "文件名" not in df1.columns or "文件名" not in df2.columns or "文件名" not in df3.columns:
    raise ValueError("One of the Excel files does not have the '文件名' column.")

# # 使用“文件名”列横向合并三个DataFrame
# df = df1.merge(df2, on="文件名", how='inner').merge(df3, on="文件名", how='inner').merge(df4, on="股票代码", how='inner')

# 合并前三个DataFrame，保持inner join方式
df_merged = df1.merge(df2, on="文件名", how='inner').merge(df3, on="文件名", how='inner')

# 将df4与合并后的DataFrame进行合并，保留所有df_merged的记录
df = df_merged.merge(df4, on="股票代码", how='left')


# 定义 name_list
name_list = [
    '在建工程', '无形资产', '商誉', '其他流动资产', '其他非流动资产', '非流动资产合计', '资产总计',
    '短期借款', '应付票据', '应付账款', '应付职工薪酬', '应交税费', '长期借款', '流动负债合计', '非流动负债合计', '负债合计',
    '股本', '销售费用', '管理费用', '研发费用', '财务费用', '投资收益',
    '营业总收入', '营业收入', '营业总成本', '营业成本',  # '净利润' , '营业外收入', '营业外支出','营业利润', '利润总额', 
]

# 将 name_list 中的列转换为数值类型，移除逗号
for name in name_list:
    df[name] = pd.to_numeric(df[name].astype(str).str.replace(',', ''), errors='coerce')
    df[name + 'new'] = ''

# 初始化 '行业名称' 列
df['行业名称'] = ''
for row in tqdm(range(len(df)), desc="处理行业名称"):
    if df.at[row, '申万行业'] != '其他':
        parts = str(df.at[row, '申万行业']).split('--')
        if len(parts) >= 2:
            df.at[row, '行业名称'] = f"{parts[0]}--{parts[1]}"
        else:
            df.at[row, '行业名称'] = df.at[row, '申万行业']
    else:
        df.at[row, '行业名称'] = str(df.at[row, '申万行业'])

# 定义新财务指标及其计算公式
new_name_list = {
    '营业成本率': {'公式': '营业成本率=营业成本/营业收入', '数值': ['营业成本', '营业收入']},
    '投资收益占营业收入比率': {'公式': '投资收益占营业收入比率=投资收益/营业收入', '数值': ['投资收益', '营业收入']},
    '管理费用率': {'公式': '管理费用率=管理费用/营业收入', '数值': ['管理费用', '营业收入']},
    '财务费用率': {'公式': '财务费用率=财务费用/营业收入', '数值': ['财务费用', '营业收入']},
    '三费比重': {'公式': '三费比重=(销售费用+管理费用+财务费用)/营业收入', '数值': ['销售费用', '管理费用', '财务费用', '营业收入']},
    '企业研发经费占费用比例': {'公式': '企业研发经费占费用比例=研发费用/(销售费用+财务费用+管理费用+研发费用)', '数值': ['研发费用', '销售费用', '财务费用', '管理费用']},
    # '企业研发经费与利润比值': {'公式': '企业研发经费与利润比值=研发费用/净利润', '数值': ['研发费用', '净利润']},
    '企业研发经费与营业收入比值': {'公式': '企业研发经费与营业收入比值=研发费用/营业收入', '数值': ['研发费用', '营业收入']},
    '研发人员占职工人数比例': {'公式': '研发人员占职工人数比例=研发人数/职工总数', '数值': ['研发人数', '职工总数']},
    '企业硕士及以上人员占职工人数比例': {'公式': '企业硕士及以上人员占职工人数比例=(硕士人员 + 博士及以上人员)/职工总数', '数值': ['硕士人员', '博士及以上人员', '职工总数']},
    '毛利率': {'公式': '毛利率=(营业收入-营业成本)/营业收入', '数值': ['营业收入', '营业成本']},
    # '营业利润率': {'公式': '营业利润率=营业利润/营业收入', '数值': ['营业利润', '营业收入']},
    '流动比率': {'公式': '流动比率=流动资产合计/流动负债合计', '数值': ['流动资产合计', '流动负债合计']},
    '速动比率': {'公式': '速动比率=(流动资产合计-存货)/流动负债合计', '数值': ['流动资产合计', '存货', '流动负债合计']},
    '资产负债比率': {'公式': '资产负债比率=负债合计/资产总计', '数值': ['负债合计', '资产总计']},
    '现金比率': {'公式': '现金比率=货币资金/流动负债合计', '数值': ['货币资金', '流动负债合计']},
    '非流动负债比率': {'公式': '非流动负债比率=非流动负债合计/负债合计', '数值': ['非流动负债合计', '负债合计']},
    '流动负债比率': {'公式': '流动负债比率=流动负债合计/负债合计', '数值': ['流动负债合计', '负债合计']},
    # '净利润率': {'公式': '净利润率=净利润/营业收入', '数值': ['净利润', '营业收入']}
}

not_list = [
    '企业研发经费占费用比例', '企业研发经费与利润比值', '企业研发经费与营业收入比值',
    '研发人员占职工人数比例', '企业硕士及以上人员占职工人数比例', '流动比率', '速动比率'
]

# 初始化新财务指标列
for new_name in new_name_list:
    df[new_name] = ''

# 逐行处理数据并计算指标
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="计算财务指标"):
    company_name = row['股票代码']
    df['年份'] = df['年份'].astype(str).str.extract(r'(\d+)').astype(int)

    year_str = str(row['年份'])
    try:
        year = int(year_str.replace('年', ''))
    except ValueError:
        print(f"无法解析年份: {year_str} 在行 {index}")
        continue

    # 计算 name_list 中每个指标
    for name in name_list:
        # 提取当前年和上一年的数值
        currency_this = df[(df['股票代码'] == company_name) & (df['年份'] == year)][name].sum()
        currency_last = df[(df['股票代码'] == company_name) & (df['年份'] == year - 1)][name].sum()

        # 如果存在缺失值，保留为NaN
        currency_this = currency_this if not pd.isna(currency_this) else None
        currency_last = currency_last if not pd.isna(currency_last) else None

        # 计算增长率
        if currency_last != None:
            growth_rate = ((currency_this - currency_last) / currency_last) * 100
            growth_rate_this = f"({currency_this} - {currency_last}) / {currency_last} * 100 = {growth_rate:.2f}%"
        else:
            growth_rate_this = '由于上一年度无数值，所以增长率为空'

        # 计算行业平均值
        industry_name = row['行业名称']
        industry_average = df[(df['年份'] == year) & (df['行业名称'] == industry_name)][name].mean()

        # 计算行业排名
        industry_data = df[(df['年份'] == year) & (df['行业名称'] == industry_name)][['股票代码', name]].dropna()
        industry_data = industry_data.groupby('股票代码')[name].sum().reset_index()
        industry_data['排名'] = industry_data[name].rank(ascending=False, method='dense')
        company_rank_series = industry_data[industry_data['股票代码'] == company_name]['排名']
        if not company_rank_series.empty:
            company_rank = int(company_rank_series.iloc[0])
        else:
            company_rank = 'N/A'

        # 更新新列
        df.at[index, name + 'new'] = {
            f"{year}年{name}": f"{currency_this}元",
            f"{year - 1}年{name}": f"{currency_last}元",
            f"{year}年{name}增长率": growth_rate_this,
            f"行业{name}平均值": f"{industry_average:.2f}元" if not pd.isna(industry_average) else "N/A",
            f"行业{name}排名": company_rank
        }

    # 计算 new_name_list 中的新指标
    for new_name in new_name_list:
        formula = new_name_list[new_name]['公式'].split('=')[1]
        values = {}

        # 提取所需数值
        # 提取所需数值
        for var in new_name_list[new_name]['数值']:
            # 获取指定条件下的 var 列，并将其转换为数值类型
            value_series = pd.to_numeric(df[(df['股票代码'] == company_name) & (df['年份'] == year)][var], errors='coerce')
            
            # 求和并处理空值情况
            value = value_series.sum()
            if pd.isna(value):
                value = 0
            
            # 将结果存入字典
            values[var] = value
            
            # 替换公式中的变量为具体数值
            formula = formula.replace(var, str(value))


        try:
            # 计算公式
            result = eval(formula)
            if new_name not in not_list:
                # 需要乘以100并添加百分号
                growth_rate_this = f"{new_name_list[new_name]['公式'].split('=')[1]} *100 = {result * 100:.2f}%"
            else:
                growth_rate_this = f"{new_name_list[new_name]['公式'].split('=')[1]} = {result:.2f}"
        except Exception as e:
            growth_rate_this = '缺少数据，所以值为空'
            print(f"计算 {new_name} 时出错: {e} 在行 {index}")

        # 创建新字典
        new_dict = {f"{year}年{var}": f"{val}元" for var, val in values.items()}
        new_dict['公式'] = new_name_list[new_name]['公式']
        new_dict[new_name] = growth_rate_this

        # 更新 DataFrame
        df.at[index, new_name] = new_dict

missing_years = [year for year in range(2018, 2024) if year not in df['年份'].unique()]
if missing_years:
    print(f"缺失年份: {missing_years}")
else:
    print("所有年份的数据都存在")

# 保存合并后的DataFrame到新的Excel文件
df.to_excel("big_data_old.xlsx", engine='openpyxl', index=False)

# 保存最终结果
df.to_excel("big_data.xlsx", engine='openpyxl', index=False)
