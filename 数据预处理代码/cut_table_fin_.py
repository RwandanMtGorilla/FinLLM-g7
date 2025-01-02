import glob
import json
import re
import pandas as pd
from multiprocessing import Pool
import os
import csv
import logging

log_dir = 'logs'
# 设置日志记录
os.makedirs(log_dir, exist_ok=True)

def setup_logger(log_filename):
    """
    配置日志记录器，将日志保存到指定的日志文件中。
    
    :param log_dir: 日志目录
    :param log_filename: 日志文件名
    """
    # 创建日志目录（如果不存在）
    os.makedirs(log_dir, exist_ok=True)
    
    # 获取日志文件的完整路径
    log_file_path = os.path.join(log_dir, log_filename)
    
    # 清空以前的处理器（防止重复日志）
    logging.getLogger().handlers = []
    
    # 添加新的处理器
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8-sig')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)
    
    # 设置日志级别
    logging.getLogger().setLevel(logging.DEBUG)

logging.basicConfig(filename=os.path.join(log_dir, 'process_log.log'), level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8-sig')

# 分割查找标记内容的函数
# 根据查找标记修改 check 的值，并结合行内容返回对应的 text
# check 用来追踪查找模式是否找到符合的文本
# check_re_1 和 check_re_2 分别是起始与结束的标记模式
def cut_all_text(check, check_re_1, check_re_2, all_text, line_dict, text):
    # logging.debug(f"[cut_all_text] 当前 check 值: {check}")
    if check == False and re.search(check_re_1, all_text):
        check = True
        # logging.debug(f"[cut_all_text] 找到匹配的 check_re_1, 修改 check 为 True")
    if check == True and line_dict['type'] not in ['页省', '页脚']:
        if not re.search(check_re_2, all_text):
            if line_dict['inside'] != '':
                text = text + line_dict['inside'] + '\n'
                # logging.debug(f"[cut_all_text] 追加文本: {line_dict['inside']}")
        else:
            check = False
            # logging.debug(f"[cut_all_text] 找到匹配的 check_re_2, 修改 check 为 False")
    return text, check  

# 处理单个文件的函数，通过各种文本处理的模式对文件内容进行筛选处理
# file_name: 要处理的文件名称
# list2: 需要查找的项目列表
def process_file(file_name, list2):
    allname = os.path.basename(file_name)
    log_filename = f"log_{allname}.txt"
    setup_logger(log_filename)
    try:
        # 打印正在处理的文件名
        logging.info(f'正在处理文件: {file_name}')
        allname = os.path.basename(file_name)
        try:
            stock, year, name = allname.split('_')
        except ValueError as e:
            # 如果文件名格式错误，返回错误
            logging.error(f'文件名格式错误: {file_name}, 错误信息: {e}')
            return None
        
        # 初始化一些变量，用来存储全文和不同表格的内容
        all_text = ''
        text1, text2, text3, text4, text5 = '', '', '', '', ''
        check1, check2, check3, check4, check5 = False, False, False, False, False
        answer_dict = {}
        
        # 为 list2 中的项目列表创建空值的对应关系
        for _l in list2:
            answer_dict[_l] = ''

        # 打开文件，将文件线进行读取处理
        log_file_name = f"log_{allname}.csv"
        log_file_path = os.path.join('logs', log_file_name)
        with open(log_file_path, mode='w', newline='', encoding='utf-8-sig') as log_file:
            writer = csv.writer(log_file)
            writer.writerow(['Line Content', 'Processing Status', 'Extracted Data'])

            with open(file_name, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                # logging.info(f"[process_file] 文件 {file_name} 读取了 {len(lines)} 行")
                for line in lines:
                    line = line.replace('\n', '')
                    log_row = [line, '']
                    try:
                        # 将线文本转化为 JSON 对象
                        line_dict = json.loads(line)
                    except json.JSONDecodeError as e:
                        # 如果解析出错，打印错误信息并记录日志
                        logging.error(f'JSON解析错误: {line}, 错误信息: {e}')
                        log_row[1] = f'JSON解析错误: {e}'
                        writer.writerow(log_row)
                        continue
                    try:
                        # 对不是 "页省" 和 "页脚" 的行内容进行添加处理
                        if line_dict['type'] not in ['页省', '页脚']:
                            all_text = all_text + line_dict['inside']
                            # logging.debug(f"[process_file] 累加 all_text: {line_dict['inside']}")
                            log_row[1] = '累加 all_text'
                            log_row.append(line_dict['inside'])

                        # 各种资产表和利润表的处理模式：通过 cut_all_text 来追踪并合并文本
                        text1, check1 = cut_all_text(check1,
                                                    '(?:财务报表.{0,150}?|1、)(?:合并资产负债表|合并及母公司资产负债表)$',
                                                    '(?:母公司资产负债表|合并及母公司利润表)$',
                                                    all_text, line_dict, text1)
                        text2, check2 = cut_all_text(check2,
                                                    '(?:财务报表.{0,15000}|2、)(?:母公司资产负债表)$',
                                                    '(?:合并利润表)$',
                                                    all_text, line_dict, text2)
                        text3, check3 = cut_all_text(check3,
                                                    '(?:财务报表.{0,15000}|3、)(?:合并利润表)$',
                                                    '(?:母公司利润表)$',
                                                    all_text, line_dict, text3)
                        text4, check4 = cut_all_text(check4,
                                                    '(?:财务报表.{0,15000}|4、)(?:母公司利润表)$',
                                                    '(?:合并现金流量表)$',
                                                    all_text, line_dict, text4)
                        text5, check5 = cut_all_text(check5,
                                                    '(?:财务报表.{0,15000}|5、)(?:合并现金流量表)$',
                                                    '(?:母公司现金流量表)$',
                                                    all_text, line_dict, text5)
                        # # 母公司现金流量表
                        # text6, check6 = cut_all_text(check6,
                        #                              '(?:负责人.{0,150000}|6、)(?:母公司现金流量表)$',
                        #                              '(?:合并所有者权益变动表)$',
                        #                              all_text, line_dict, text6)
                        # # 合并所有者权益变动表
                        # text7, check7 = cut_all_text(check7,
                        #                              '(?:负责人.{0,150000}|7、)(?:合并所有者权益变动表)$',
                        #                              '(?:母公司所有者权益变动表)$',
                        #                              all_text, line_dict, text7)
                        # # 母公司所有者权益变动表
                        # text8, check8 = cut_all_text(check8,
                        #                              '(?:负责人.{0,150000}|8、)(?:母公司所有者权益变动表)$',
                        #                              '(?:公司基本情况)$',
                        #                              all_text, line_dict, text8)
                        if re.search('(?:财务报表.{0,15000}|6、)(?:母公司现金流量表)$', all_text):
                            logging.info(f"[process_file] 找到 '母公司现金流量表'，提前结束处理")
                            log_row[1] = "找到 '母公司现金流量表'，提前结束处理"
                            writer.writerow(log_row)
                            break
                        # if re.search('(?:财务报表.{0,15000}|6、)(?:母公司现金流量表)$', all_text):
                        #     logging.info(f"[process_file] 找到 '母公司现金流量表'，提前结束处理")
                        #     log_row[1] = "找到 '母公司现金流量表'，提前结束处理"
                        #     writer.writerow(log_row)
                        #     break

                    except Exception as e:
                        logging.error(f'处理行时出错: {line_dict}, 错误信息: {e}')
                        log_row[1] = f'处理行时出错: {e}'
                        pass

                    writer.writerow(log_row)

        # 取合并资产负债表，合并利润表和现金流量表的文本的方式
        cut1_len = len(text1.split('合并资产负债表')[0])
        cut2_len = len(text2.split('母公司资产负债表')[0])
        cut3_len = len(text3.split('合并利润表')[0])
        cut4_len = len(text4.split('母公司利润表')[0])
        cut5_len = len(text5.split('合并现金流量表')[0])

        logging.debug(f"[process_file] 合并资产负债表文本截取长度: {cut1_len}")
        logging.debug(f"[process_file] 合并利润表文本截取长度: {cut3_len}")
        logging.debug(f"[process_file] 合并现金流量表文本截取长度: {cut5_len}")


        cut1_len_ = len(text1)
        cut2_len_ = len(text2)
        cut3_len_ = len(text3)
        cut4_len_ = len(text4)
        cut5_len_ = len(text5)

        logging.debug(f"[process_file] 合并资产负债表文本实际长度: {cut1_len_}")
        logging.debug(f"[process_file] 母公司资产负债表文本实际长度: {cut2_len_}")
        logging.debug(f"[process_file] 合并利润表文本实际长度: {cut3_len_}")
        logging.debug(f"[process_file] 母公司利润表文本实际长度: {cut4_len_}")
        logging.debug(f"[process_file] 合并现金流量表文本实际长度: {cut5_len_}")

        # 对查找的资产返回结果进行处理并结果存入 answer_dict
        def check_data(answer_dict, text_check, addwords, stop_re):
            logging.info(f"[check_data] 处理文本，附加词: {addwords}, 结束正则: {stop_re}")
            text_list = text_check.split('\n')
            data = []
            check_len = 0
            for _t in text_list:
                logging.debug(f"[check_data] 当前处理文本: {_t}")
                if re.search("\['项目", _t) and not re.search("调整数", _t) and check_len == 0:
                    check_len = len(eval(_t))
                    logging.debug(f"[check_data] 设置 check_len 为: {check_len}")
                if re.search('^[\[]', _t):
                    try:
                        # 将文本转化为 list，并进行文本变换
                        text_l = eval(_t)
                        text_l[0] = text_l[0].replace(' ', '').replace('(', '（').replace(')', '）')\
                            .replace(':', '：').replace('／', '/')
                        cut_re = re.match('(?:[一-龥]、|（[一-龥]）|\d\.|\u52a0：|\u51cf：|\u5176中：|\uff08元/股）)', text_l[0])
                        if cut_re:
                            text_l[0] = text_l[0].replace(cut_re.group(), '')
                            logging.debug(f"[check_data] 匹配到数据项: {text_l}")
                        else:
                            logging.debug(f"[check_data] 匹配失败 数据项: {text_l}")
                        text_l[0] = text_l[0].split('（')[0]
                        # 如果长度等于无误的查找条目，则将其加入数据
                        if check_len != 0  and re.search('[一-龥]', text_l[0]): # and check_len == len(text_l)
                            data.append(text_l)
                            logging.debug(f"[check_data] 添加数据项: {text_l}")
                        else:
                            logging.debug(f"[check_data] {check_len != 0} and {check_len == len(text_l)} and {re.search('[一-龥]', text_l[0])} 数据检验不通过")
                    except Exception as e:
                        logging.error(f'处理文本时出错: {_t}, 错误信息: {e}')
                if data != [] and re.search(stop_re, _t):
                    logging.info(f"[check_data] 找到结束标志，停止数据收集")
                    break

            # 如果有有效数据，则转化为 DataFrame，并替换 answer_dict 中相关输出
            if data != []:
                df = pd.DataFrame(data[1:], columns=data[0])
                df.replace('', '无', inplace=True)
                logging.info(f"[check_data] 创建 DataFrame，列: {df.columns}")
                if year + "年" + addwords in df.columns and '项目' in df.columns:
                    df = df.drop_duplicates(subset='项目', keep='first')
                    logging.info(f"[check_data] 当前 DataFrame {df}")
                    for key in answer_dict:
                        try:
                            match_answer = df[df['项目'] == key]
                            if not match_answer.empty:
                                if answer_dict[key] == '':
                                    answer_dict[key] = match_answer[year + "年" + addwords].values[0]
                                    logging.debug(f"[check_data] 更新 answer_dict[{key}] = {answer_dict[key]}")
                        except Exception as e:
                            logging.error(f'匹配项目时出错: {key}, 错误信息: {e}')
            return answer_dict

        # 查找资产和返回的各类表格数据
        answer_dict = check_data(answer_dict, text1[cut1_len:], '12月31日', '合并资产负债表')
        # answer_dict = check_data(answer_dict, text2[cut2_len:], '度', '母公司资产负债表')
        answer_dict = check_data(answer_dict, text3[cut3_len:], '度', '合并利润表')
        answer_dict = check_data(answer_dict, text4[cut4_len:], '度', '母公司利润表')
        answer_dict = check_data(answer_dict, text5[cut5_len:], '度', '合并现金流量表')
        
        # 创建新的数据行，将所有对应项目带入
        new_row = {
            '文件名': allname,
            '日期': '', '公司名称': name, '股票代码': stock, '年份': year, '类型': '年度报告',
            '合并资产负债表': text1[cut1_len:], '合并利润表': text3[cut3_len:], '合并现金流量表': text5[cut5_len:], '全文': str(lines)}
        for key in answer_dict:
            new_row[key] = answer_dict[key]
        # logging.info(f"结束 {file_name}, 提取数据: {new_row}")

        # 生成日志文件
        with open(log_file_path, mode='a', newline='', encoding='utf-8-sig') as log_file:
            writer = csv.writer(log_file)
            writer.writerow(['提取的数据', ''])
            writer.writerow(new_row.keys())
            writer.writerow(new_row.values())
        logging.info(f"日志已保存到 {log_file_path}")
        
        return new_row
    except Exception as e:
        # 创建新的数据行，将所有对应项目带入
        new_row = {
            '文件名': file_name,
            '日期': '', '公司名称': '', '股票代码': '', '年份': '', '类型': '年度报告',
            '合并资产负债表': '', '合并利润表': '', '合并现金流量表': '', '全文': ''}
        for key in answer_dict:
            new_row[key] = answer_dict[key]
        logging.info(f"结束 {file_name}, 处理遇到错误: {e}")

        return new_row



# 运行主函数，为文件夹中的文件执行处理
if __name__ == "__main__":
    # 文件夹路径
    folder_path = os.path.join('..', 'alltxt2')
    # 获取文件夹内所有文件名称
    file_names = glob.glob(folder_path + '\*')
    # file_names = sorted(file_names, reverse=True)[:5]
    logging.info(f'找到的文件: {file_names}')
    results = []

    # 列表中就是要根据与包含的一些行的标题
    list1 = [
        '文件名', '日期', '公司名称', '股票代码', '年份', '类型',
        '合并资产负债表', '合并利润表', '合并利润表', '全文']
    list2 = [
        '货币资金', '结算备付金', '抵出资金', '交易性金融资产', '以公允价值计量且其变动计入当期损益的金融资产', '衍生金融资产', '应收票据',
        '应收账款', '应收款项融资', '预付款项', '应收保费', '应收分保账款', '应收分保合同准备金', '其他应收款', '应收利息', '应收股利',
        '买入返售金融资产', '存货', '合同资产', '持有待售资产', '一年内到期的非流动资产', '其他流动资产',
        '流动资产合计',
        '发放贷款和垫款', '债权投资', '可供出售的金融资产', '其他债权投资', '持有至到期投资', '长期应收款', '长期股权投资',
        '其他权益工具投资', '其他非流动金融资产', '投资性房地产', '固定资产', '在建工程', '生产性生物资产', '油气资产', '使用权资产',
        '无形资产', '开发支出', '商誉', '长期待推费用', '遞延所得税资产', '其他非流动资产',
        '非流动资产合计', '资产总计',
        '短期借款', '向中央银行借款', '抵入资金', '交易性金融负债', '以公允价值计量且其变动计入当期损益的金融负债', '衍生金融负债',
        '应付票据', '应付账款', '预收款项', '合同负债', '卖出回购金融资产款', '吸收存款及同业存放', '代理买卖证券款', '代理承销证券款',
        '应付职工薪酬', '应交税费', '其他应付款', '应付利息', '应付股利', '应付手续费及佣金', '应付分保账款', '持有待售负债',
        '一年内到期的非流动负债', '其他流动负债', '流动负债合计',
        '保险合同准备金', '长期借款', '应付债券', '租赁负债', '长期应付款', '长期应付职工薪酬', '预计负债', '遞延收益', '遞延所得税负债',
        '其他非流动负债', '非流动负债合计', '负债合计',
        '股本', '实收资本', '其他权益工具', '资本公积', '库存股', '其他综合收益', '专项储备', '盈余公积', '一般风险准备', '未分配利润',
        '归属于母公司所有者权益合计', '少数股东权益', '所有者权益合计', '负债和所有者权益总计',
        '营业总收入', '营业收入', '利息收入', '已赚保费', '手续费及佣金收入',
        '营业总成本', '营业成本', '利息支出', '手续费及佣金支出',
        '退保金', '赔付支出净额', '提取保险责任合同准备金净额', '保单红利支出', '分保费用', '税金及附加',
        '销售费用', '管理费用', '研发费用', '财务费用', '利息费用', '其他收益',
        '投资收益', '其中：对现金', '经营活动现金流入小计', '购买商品、接受劳务支付的现金', '客户贷款及垫款净增加额',
        '存放中央银行和同业款项净增加额', '支付原保险合同赔付款项的现金', '抵出资金净增加额', '支付利息、手续费及佣金的现金',
        '支付保单红利的现金', '支付给职工以及为职工支付的现金', '支付的各项税费', '支付其他与经营活动有关的现金',
        '经营活动现金流出小计', '经营活动产生的现金流量净额',
        '收回投资收到的现金', '取得投资收益收到的现金', '处置固定资产、无形资产和其他长期资产收回的现金净额',
        '处置子公司及其他营业单位收到的现金净额', '收到其他与投资活动有关的现金', '投资活动现金流入小计',
        '购建固定资产、无形资产和其他长期资产支付的现金', '投资支付的现金', '质押贷款净增加额', '取得子公司及其他营业单位支付的现金净额',
        '支付其他与投资活动有关的现金', '投资活动现金流出小计', '投资活动产生的现金流量净额',
        '吸收投资收到的现金', '子公司吸收少数股东投资收到的现金', '取得借款收到的现金', '收到其他与策资活动有关的现金',
        '策资活动现金流入小计', '偿还债务支付的现金', '分配股利、利润或偿付利息支付的现金', '子公司支付给少数股东的股利、利润',
        '支付其他与策资活动有关的现金', '策资活动现金流出小计', '策资活动产生的现金流量净额',
        '汇率变动对现金及现金等价物的影响',
        '现金及现金等价物净增加额', '期初现金及现金等价物余额', '期末现金及现金等价物余额']
    all_list = list1 + list2
    df = pd.DataFrame(columns=all_list)

    # 运行并发，将文件输入到 process_file 中处理
    with Pool(processes=16) as pool:
        results = pool.starmap(process_file, [(file_name, list2) for file_name in file_names])

    # 过滤掉为 None 的结果
    results = [res for res in results if res is not None]
    print(f'处理后的结果: {results}')

    # 如果有有效的结果，则将结果保存到 Excel
    if results:
        df = pd.DataFrame(results)
        df.to_excel("big_data2.xlsx", index=False)
    else:
        print("没有有效的处理结果。")    