import logging
import csv
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import List, Optional
from openai import OpenAI
from config import OPENAI_API_KEY, OPENAI_API_BASE
import uvicorn

import re
import json

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import TransportError, ConnectionError, NotFoundError, RequestError
from tqdm import tqdm
import logging

import os
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

import uuid
# 手动加载字体文件
font_path = './SimHei.ttf'  # 请确保该路径下有 SimHei.ttf 字体文件
font_prop = fm.FontProperties(fname=font_path)
font_prop.set_size(20)  # 设置字体大小为 20

# 全局字体设置为 SimHei
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['font.family'] = font_prop.get_name()  # 强制使用 SimHei
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示为方块的问题
plt.rcParams['font.size'] = 16 

# 定义紫色系的颜色列表
purple_colors = ['#8B5FBF', '#A485E2', '#C89BFF', '#9370DB', '#BA55D3', '#DA70D6']

# 确保保存图像的文件夹存在
os.makedirs('./image', exist_ok=True)

def generate_chart_from_df(df):
    # 定义存储 URL 的列表
    image_urls = []
    
    # 检查是否包含年份数据
    if '年份' not in df.columns or '中文简称' not in df.columns:
        print("数据表中缺少必要的列：'年份' 和 '中文简称'")
        return

    # 去除小数点并转换年份列为整数，然后再转为字符串（确保绘图时按文本处理）
    df['年份'] = df['年份'].astype(int).astype(str)

    # 获取所有可能的年份范围，并确保年份排序正确
    all_years = sorted(df['年份'].unique(), key=lambda x: int(x))  # 按数值顺序排序

    # 按公司名称分组数据
    grouped = df.groupby('中文简称')
    companies = grouped.groups.keys()
    
    # 如果只有一个公司则不在同一图上画多个
    if len(companies) < 2:
        for company, group_data in grouped:
            image_urls.extend(plot_single_company(group_data, company))
        return ','.join(image_urls)
    
    # 检查该公司数据中除了年份和中文简称列外的其他列作为图表内容
    data_columns = [col for col in df.columns if col not in ['年份', '中文简称']]
    
    # 如果没有可用数据列，退出
    if not data_columns:
        return

    # 遍历数据列，生成图表
    for data_column in data_columns:
        plt.figure(figsize=(10, 6))
        
        for i, (company, group_data) in enumerate(grouped):
            # 创建一个包含所有年份的数据框架，填充缺失年份的数据为空
            company_data = group_data[['年份', data_column]].set_index('年份').reindex(all_years)
            company_data[data_column] = pd.to_numeric(company_data[data_column], errors='coerce')

            # 确保按年份顺序排序
            company_data = company_data.sort_index()

            color = purple_colors[i % len(purple_colors)]  # 使用紫色调，循环选择颜色

            plt.plot(all_years, company_data[data_column], marker='o', linestyle='-', markersize=8, linewidth=3, color=color, label=company)
        
        plt.title(f"{data_column}变化", fontproperties=font_prop)
        plt.xlabel("年份", fontproperties=font_prop)
        plt.ylabel(data_column, fontproperties=font_prop)
        plt.legend(prop=font_prop)  # 显示图例以区分公司
        
        # 保存图片到 image 文件夹，并生成 UUID 文件名
        filename = f"{uuid.uuid4()}.png"
        image_path = os.path.join('./image', filename)
        plt.savefig(image_path, bbox_inches='tight')
        plt.close()

        # 生成URL，假设服务器在特定IP和端口上运行
        image_url = f"http://118.89.133.59:5753/images/{filename}"
        image_urls.append(f"![{data_column}变化]({image_url})")

        print(f"图像已保存：{image_path}")

    # 返回所有图像 URL 的字符串
    return ','.join(image_urls)

def plot_single_company(group_data, company):
    """生成单个公司数据的图表并返回 URL 列表"""
    data_columns = [col for col in group_data.columns if col not in ['年份', '中文简称']]
    image_urls = []
    
    for data_column in data_columns:
        cleaned_data = group_data[['年份', data_column]].dropna()
        cleaned_data[data_column] = pd.to_numeric(cleaned_data[data_column], errors='coerce')
        cleaned_data = cleaned_data.dropna()  # 去除转换为数值类型后仍然无效的数据

        # 确保年份排序正确
        cleaned_data = cleaned_data.sort_values(by='年份')

        if cleaned_data.empty:
            # 如果清理后数据为空，跳过该列
            continue

        plt.figure(figsize=(10, 6))
        plt.plot(cleaned_data['年份'], cleaned_data[data_column], marker='o', linestyle='-', linewidth=2, color=purple_colors[0])
        
        plt.title(f"{company} - {data_column}变化", fontproperties=font_prop)
        plt.xlabel("年份", fontproperties=font_prop)
        plt.ylabel(data_column, fontproperties=font_prop)
        
        # 保存图片到 image 文件夹
        filename = f"{uuid.uuid4()}.png"
        image_path = os.path.join('./image', filename)
        plt.savefig(image_path, bbox_inches='tight')
        plt.close()

        # 生成URL，假设服务器在特定IP和端口上运行
        image_url = f"http://118.89.133.59:5753/images/{filename}"
        image_urls.append(f"![{company} - {data_column}变化]({image_url})")

        print(f"图像已保存：{image_path}")

    return image_urls



def execute_sql_query(es, query):
    """
    执行 SQL 查询并返回结果。

    Args:
        es (Elasticsearch): Elasticsearch 客户端实例。
        query (str): SQL 查询字符串。

    Returns:
        pd.DataFrame: 查询结果的 DataFrame。
    """
    try:
        response = es.sql.query(body={"query": query})
        columns = [col['name'] for col in response['columns']]
        rows = response['rows']
        df = pd.DataFrame(rows, columns=columns)
        return df
    except (TransportError, ConnectionError, NotFoundError, RequestError) as e:
        print(f"执行 SQL 查询时出错: {e}")
        logging.error(f"执行 SQL 查询时出错: {e}")
        return None


# 初始化 OpenAI 客户端
client = OpenAI(
    api_key=OPENAI_API_KEY,
    base_url=OPENAI_API_BASE,
)

PROMPT_DICT = {
    "default": {
        "system_prompt": "You are a helpful assistant.",
        "prompt": "Now the user input = {user_input}, the reference is {reference}, now please answer.",
        "max_tokens": 500,
        "temperature": 0,
    },  
    "default": {
        "system_prompt": (
            "You are a ElasticSearch expert.\n"
            "Please help to generate a SQL query to answer the question. Your response should ONLY be based on the given context and follow the response guidelines and format instructions.\n"
            "=== Response Guidelines \n"
            "1. If the provided context is sufficient, please generate a valid SQL query without any explanations for the question. \n"
            "2. If the provided context is almost sufficient but requires knowledge of a specific string in a particular column, please generate an intermediate SQL query to find the distinct strings in that column. Prepend the query with a comment saying intermediate_sql \n"
            "3. If the provided context is insufficient, please explain why it can't be generated. \n"
            "4. Please use the most relevant table(s). \n"
            "5. If the question has been asked and answered before, please repeat the answer exactly as it was given before. \n"
            "6. Ensure that the output SQL is ElasticSearch_8.x-compliant and executable, and free of syntax errors. \n"
            "当前数据库存储了2018-2019年上海证券交易所所有A股年报信息,只有一个表。有可查询字段如下\n"
            "**基础信息**\n"
            "文件名	企业全称	公司简称	年份	股票代码	证券简称	行业名称	电子信箱	注册地址	办公地址	企业名称	中文简称 	外文名称	外文名称缩写	公司网址	法定代表人	研发人数	职工总数	销售人员	技术人员	硕士人员	博士及以上人员\n"
            "**财务数据**\n"
            "货币资金	应收款项融资	存货	其他非流动金融资产	固定资产	无形资产	资产合计	总资产	应付职工薪酬	负债合计	总负债	交易性金融资产	应收票据	应收账款	流动资产合计	流动资产	在建工程	商誉	其他流动资产	其他非流动资产	非流动资产合计	非流动资产	短期借款	应付票据	应付账款	应交税费	长期借款	流动负债合计	流动负债	非流动负债合计	非流动负债	股本	营业总收入	营业收入	营业总成本	营业成本	销售费用	管理费用	研发费用	财务费用	投资收益	营业外收入	营业外支出	营业利润	利润合计 	净利润\n"
            "**长文本**\n"
            "审计意见	关键审计事项	主要会计数据和财务指标	主要销售客户	主要供应商	研发投入	现金流	资产及负债状况	重大资产和股权出售	主要控股参股公司分析	公司未来发展的展望	合并报表范围发生变化的情况说明	聘任、解聘会计师事务所情况	面临退市情况	破产重整相关事项	重大诉讼、仲裁事项	处罚及整改情况	公司及其控股股东、实际控制人的诚信状况	重大关联交易	重大合同及其履行情况	重大环保问题	社会责任情况	公司董事、监事、高级管理人员变动情况	公司员工情况	非标准审计报告的说明	公司控股股东情况	    合并资产负债表	合并利润表	合并现金流量表	审计报告	流动负债合计new	非流动负债合计new\n"
            "**计算值**\n"
            "在建工程new	无形资产new	商誉new	其他流动资产new	其他非流动资产new	非流动资产合计new	资产总计new	短期借款new	应付票据new	应付账款new	应付职工薪酬new	应交税费new	长期借款new	流动负债new	非流动负债new	负债合计new	股本new	营业总收入new	营业收入new	营业总成本new	营业成本new	销售费用new	管理费用new	研发费用new	财务费用new	投资收益new	营业外收入new	营业外支出new	营业利润new	利润合计new	净利润new	营业成本率	投资收益占营业收入比率	管理费用率	财务费用率	三费比重	企业研发经费占费用比例	企业研发经费与利润比值	企业研发经费与营业收入比值	研发人员占职工人数比例	企业硕士及以上人员占职工人数比例	毛利率	营业利润率	流动比率	速动比率	资产负债比率	现金比率	非流动负债比率	流动负债比率	净利润率\n"
            "**不常见字段**\n"
            "结算备付金	以公允价值计量且其变动计入当期损益的金融资产	衍生金融资产	预付款项	应收保费	应收分保账款	应收分保合同准备金	其他应收款	应收利息	应收股利	买入返售金融资产	合同资产	持有待售资产	一年内到期的非流动资产	发放贷款和垫款	债权投资	其他债权投资	持有至到期投资	长期应收款	长期股权投资	其他权益工具投资	投资性房地产	生产性生物资产	油气资产	使用权资产	开发支出	资产总计	向中央银行借款		交易性金融负债	以公允价值计量且其变动计入当期损益的金融负债	衍生金融负债	预收款项	合同负债	卖出回购金融资产款	吸收存款及同业存放	代理买卖证券款	代理承销证券款	其他应付款	应付利息	应付股利	应付手续费及佣金	应付分保账款	持有待售负债	一年内到期的非流动负债	其他流动负债	保险合同准备金	应付债券	租赁负债	长期应付款	长期应付职工薪酬	预计负债	其他非流动负债	实收资本	其他权益工具	资本公积	库存股	其他综合收益	专项储备	盈余公积	一般风险准备	未分配利润	归属于母公司所有者权益合计	少数股东权益	所有者权益合计	负债和所有者权益总计	利息收入	已赚保费	手续费及佣金收入	利息支出	手续费及佣金支出	退保金	赔付支出净额 保单红利支出	分保费用	税金及附加	利息费用	其他收益	经营活动现金流入小计	购买商品、接受劳务支付的现金	客户贷款及垫款净增加额	存放中央银行和同业款项净增加额	支付原保险合同赔付款项的现金	支付利息、手续费及佣金的现金	支付保单红利的现金	支付给职工以及为职工支付的现金	支付的各项税费	支付其他与经营活动有关的现金	经营活动现金流出小计	经营活动产生的现金流量净额	收回投资收到的现金	取得投资收益收到的现金	处置固定资产、无形资产和其他长期资产收回的现金净额	处置子公司及其他营业单位收到的现金净额	收到其他与投资活动有关的现金	投资活动现金流入小计	购建固定资产、无形资产和其他长期资产支付的现金	投资支付的现金	质押贷款净增加额	取得子公司及其他营业单位支付的现金净额	支付其他与投资活动有关的现金	投资活动现金流出小计	投资活动产生的现金流量净额	吸收投资收到的现金	子公司吸收少数股东投资收到的现金	取得借款收到的现金	偿还债务支付的现金	分配股利、利润或偿付利息支付的现金	子公司支付给少数股东的股利、利润	汇率变动对现金及现金等价物的影响	现金及现金等价物净增加额	期初现金及现金等价物余额	期末现金及现金等价物余额\n"
            "示例查询：\n"
            "(单数据查询)\n"
            "```sql\n"
            "SELECT \"中文简称\", \"货币资金\"\n"
            "FROM testindex\n"
            "WHERE \"股票代码\" = \'603429\' AND \"年份\" = \'2023\'\n"
            "WHERE \"年份\" = \'2023\'\n"
            "```\n"
            "(连续数据查询)\n"
            "```sql\n"
            "SELECT \"中文简称\", \"年份\", \"职工总数\"\n"
            "FROM testindex\n"
            "WHERE \"股票代码\" = \'603429\'\n"
            "ORDER BY \"年份\" ASC\n"
            "LIMIT 10\n"
            "```\n"
            "请注意，查询时一定要使用股票代码进行匹配。并务必select中文简称。名称对应的股票代码将在reference中给出。另外，今年是2024年。"
            #todo
            ),

        "prompt": "Now the user input: {user_input}, the reference is {reference}, now please answer.",
        "max_tokens": 500,
        "temperature": 0,
    },  
}                    

import pandas as pd
import re
import time  # 导入 time 模块

import pandas as pd
import re
import time  # 导入 time 模块

# 读取 Excel 文件中的映射关系
def load_mapping(file_path):
    df = pd.read_excel(file_path)
    # 将 security_name 和 security_code 存储为字典，方便查找
    mapping = dict(zip(df['security_name'], df['security_code']))
    return mapping

# 根据输入的字符串返回对应的映射关系，并记录查询时间
def find_security_mapping(mapping, query_string):
    # 记录查询开始时间
    start_time = time.time()

    # 正则表达式匹配所有可能的 security_name（使用 .+ 匹配部分子串）
    matched_names = []
    for name in mapping.keys():
        # 使用 re.search 来实现模糊匹配，忽略大小写
        if re.search(re.escape(name), query_string, re.IGNORECASE):
            matched_names.append(name)
    
    # 如果找到了匹配的 security_name, 则输出对应的映射关系
    result = []
    for name in matched_names:
        security_code = mapping[name]
        result.append(f"{name} = {security_code}")
    
    # 记录查询结束时间并计算查询所用的时间
    end_time = time.time()
    query_time = end_time - start_time
    
    # 输出查询所用时间
    print(f"查询时间: {query_time:.4f} 秒")
    
    return ", ".join(result)



def extract_json_content(input_str):
    # 去除前后空白字符
    input_str = input_str.strip()
    # 定义正则表达式模式
    patterns = [
        # 带有语言标识的代码块
        re.compile(r'^```sql\s*\n(.*?)\n```$', re.DOTALL | re.IGNORECASE),
        # 不带语言标识的代码块
        re.compile(r'^```\s*\n(.*?)\n```$', re.DOTALL),
        # 纯 JSON 字符串
        re.compile(r'^(.*?)$', re.DOTALL)
    ]
    for pattern in patterns:
        match = pattern.match(input_str)
        if match:
            sql_content = match.group(1).strip()
            # 验证是否为有效的 JSON
            try:
                # json_object = json.loads(json_content)
                # 如果需要返回解析后的 JSON 对象，可以返回 json_object
                # 这里返回 JSON 字符串
                return sql_content
            except Exception as e:
                # 如果内容不是有效的 JSON，继续尝试下一个模式
                continue
    return input_str

# 日志设置
logging.basicConfig(filename="api_logs.csv", level=logging.INFO, format="%(message)s")

def log_to_csv(data):
    # 创建 CSV 文件，如果不存在则创建，并写入表头
    file_exists = False
    try:
        with open("api_logs_report.csv", mode="a", newline='', encoding="utf-8-sig") as file:
            writer = csv.writer(file)
            if file.tell() == 0:
                # 写入表头
                writer.writerow(["Timestamp", "Input Type", "User Input", "History Messages", "Image URL", "Output Answer", "Response Data"])
            # 写入日志数据
            writer.writerow(data)
    except Exception as e:
        logging.error(f"Failed to write to CSV: {e}")

# ChatGPT 交互函数
def chat_with_model(type, user_input, mapping, history_messages=None, image=None, timeout=10):
    prompt_config = PROMPT_DICT.get(type, PROMPT_DICT["default"])
    sys_prompt = prompt_config["system_prompt"]
    reference = find_security_mapping(mapping, user_input)  # 从本地获取 reference
    prompt = prompt_config["prompt"].format(user_input=user_input, reference=reference)
    max_tokens = prompt_config["max_tokens"]
    temperature = prompt_config["temperature"]

    # if history_messages is None:
        # history_messages = [{"role": "system", "content": sys_prompt}]
    history_messages = [{"role": "system", "content": sys_prompt}]
    if image:
        url = image
        history_messages.append({
            "role": "user", 
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": url}}
            ]
        })
    else:
        history_messages.append({"role": "user", "content": prompt})
    try:
        response = client.chat.completions.create(
            model = 'chatgpt-4o-latest',
            messages = history_messages,
            max_tokens = max_tokens,
            temperature = temperature,
        )

        answer = response.choices[0].message.content
        answer = extract_json_content(answer)
        history_messages.append({"role": "assistant", "content": answer})

        try:
            data = json.dumps(answer, ensure_ascii=False, indent=4)
        except Exception as e:
            data = e
            print(f"error: {e}")
    except Exception as e:
        answer = ""
        data = e
        print(f"error: {e}")
    # 获取当前时间
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 将输入输出信息记录到日志中
    log_data = [
        timestamp, 
        type, 
        user_input, 
        json.dumps(history_messages, ensure_ascii=False), 
        image, 
        answer, 
        data
    ]
    log_to_csv(log_data)

    return data, answer, history_messages

def get_es_client():
    es_host = 'http://localhost:9200'
    es = Elasticsearch([es_host])

    # 检查连接是否成功
    if not es.ping():
        print("无法连接到 Elasticsearch。")
        logging.error("无法连接到 Elasticsearch。")
        return
    return es

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import uuid

# 假设这些函数已经定义了
# load_mapping, chat_with_model, get_es_client, execute_sql_query, generate_chart_from_df

def process_user_input(user_input):
    # 加载映射关系
    file_path = "security_code_mapping.xlsx"
    mapping = load_mapping(file_path)

    # 配置 Elasticsearch 客户端
    es = get_es_client()
    
    # 执行查询，获取数据
    data, query, history_messages = chat_with_model("type", user_input, mapping, history_messages=None, image=None, timeout=10)
    answer = execute_sql_query(es, query)
    
    # 格式化返回的 answer 数据
    formatted_answer = f"数据库查询到数据:\n```\n{answer}\n```"

    # 尝试生成图像，并格式化 reference
    try:
        reference = generate_chart_from_df(answer)
        if reference:
            formatted_reference = f"生成了图像{reference},请在回答中展示。"
        else:
            formatted_reference = ""
    except Exception as e:
        print(f"图像生成失败: {e}")
        formatted_reference = ""

    # 生成并返回最终字符串
    final_output = f"{formatted_answer}\n{formatted_reference}".strip()
    return final_output

# 示例调用
if __name__ == "__main__":
    user_input = "汇顶科技，上海亚虹，近几年营业成本多少？"
    output = process_user_input(user_input)
    print(output)
