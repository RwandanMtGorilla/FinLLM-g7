import glob
import json
import re
import pandas as pd
# from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import os

def cut_all_text(check, check_re_1, check_re_2, all_text, line_dict, text):
    if not check and re.search(check_re_1, all_text):
        check = True
        print(f"匹配到开始模式: {check_re_1}")
    if check and line_dict['type'] not in ['页眉', '页脚']:
        if not re.search(check_re_2, all_text):
            if line_dict['inside']:
                text += line_dict['inside'] + '\n'
        else:
            check = False
            print(f"匹配到结束模式: {check_re_2}")
    return text, check

def process_file(file_name):
    # print('开始 ', file_name.replace('\n', ''))
    # allname = file_name.split('\\')[-1]

    # allname = os.path.basename(file_name)

    # date, name, stock, short_name, year, else1 = allname.split('__')
    allname = os.path.basename(file_name)
    parts = allname.split('_')
    if len(parts) > 2:
        # 前五项固定，多的部分合并到第六项
        stock, year = parts[:2]
        name = "_".join(parts[2:])  # 将剩余部分合并到第六项
    else:
        # 如果不够六项，填充空值
        stock, year = parts + [''] * (2 - len(parts))
        name = ''  # 没有多余部分

    all_text = ''
    text1, text2, text3, text4, text5, text6, text7, text8 = '', '', '', '', '', '', '', ''
    check1, check2, check3, check4, check5, check6, check7, check8 = False, False, False, False, False, False, False, False
    answer_dict = {}
    list2 = [
        '货币资金', '结算备付金', '拆出资金', '交易性金融资产', '以公允价值计量且其变动计入当期损益的金融资产',
        '衍生金融资产', '应收票据',
        '应收账款', '应收款项融资', '预付款项', '应收保费', '应收分保账款', '应收分保合同准备金', '其他应收款',
        '应收利息', '应收股利',
        '买入返售金融资产', '存货', '合同资产', '持有待售资产', '一年内到期的非流动资产', '其他流动资产',
        '流动资产合计',
        '发放贷款和垫款', '债权投资', '可供出售金融资产', '其他债权投资', '持有至到期投资', '长期应收款',
        '长期股权投资',
        '其他权益工具投资', '其他非流动金融资产', '投资性房地产', '固定资产', '在建工程', '生产性生物资产', '油气资产',
        '使用权资产',
        '无形资产', '开发支出', '商誉', '长期待摊费用', '递延所得税资产', '其他非流动资产',
        '非流动资产合计', '资产总计',
        '短期借款', '向中央银行借款', '拆入资金', '交易性金融负债', '以公允价值计量且其变动计入当期损益的金融负债',
        '衍生金融负债',
        '应付票据', '应付账款', '预收款项', '合同负债', '卖出回购金融资产款', '吸收存款及同业存放', '代理买卖证券款',
        '代理承销证券款',
        '应付职工薪酬', '应交税费', '其他应付款', '应付利息', '应付股利', '应付手续费及佣金', '应付分保账款',
        '持有待售负债',
        '一年内到期的非流动负债', '其他流动负债', '流动负债合计',
        '保险合同准备金', '长期借款', '应付债券', '租赁负债', '长期应付款', '长期应付职工薪酬', '预计负债', '递延收益',
        '递延所得税负债',
        '其他非流动负债', '非流动负债合计', '负债合计',
        '股本', '实收资本', '其他权益工具', '资本公积', '库存股', '其他综合收益', '专项储备', '盈余公积',
        '一般风险准备', '未分配利润',
        '归属于母公司所有者权益合计', '少数股东权益', '所有者权益合计', '负债和所有者权益总计',
        '营业总收入', '营业收入', '利息收入',
        '已赚保费', '手续费及佣金收入',
        '营业总成本', '营业成本', '利息支出', '手续费及佣金支出',
        '退保金', '赔付支出净额', '提取保险责任合同准备金净额', '保单红利支出', '分保费用', '税金及附加',
        '销售费用', '管理费用', '研发费用', '财务费用', '利息费用', '其他收益',
        '投资收益', '其中：对联营企业和合营企业的投资收益', '以摊余成本计量的金融资产终止确认收益', '汇兑收益',
        '净敞口套期收益',
        '公允价值变动收益', '信用减值损失', '资产减值损失', '资产处置收益',
        '营业利润', '营业外收入', '营业外支出', '利润总额', '所得税费用',
        '净利润', '按经营持续性分类', '持续经营净利润', '终止经营净利润', '按所有权归属分类',
        '归属于母公司所有者的净利润', '少数股东损益',
        '其他综合收益的税后净额', '归属母公司所有者的其他综合收益的税后净额', '不能重分类进损益的其他综合收益',
        '重新计量设定受益计划变动额', '权益法下不能转损益的其他综合收益', '其他权益工具投资公允价值变动',
        '企业自身信用风险公允价值变动', '其他',
        '将重分类进损益的其他综合收益', '权益法下可转损益的其他综合收益', '其他债权投资公允价值变动',
        '可供出售金融资产公允价值变动损益',
        '金融资产重分类计入其他综合收益的金额', '持有至到期投资重分类为可供出售金融资产损益',
        '其他债权投资信用减值准备', '现金流量套期储备', '外币财务报表折算差额', '其他',
        '归属于少数股东的其他综合收益的税后净额',
        '综合收益总额', '归属于母公司所有者的综合收益总额', '归属于少数股东的综合收益总额',
        '基本每股收益', '稀释每股收益',
        '销售商品、提供劳务收到的现金', '客户存款和同业存放款项净增加额', '向中央银行借款净增加额',
        '向其他金融机构拆入资金净增加额', '收到原保险合同保费取得的现金', '收到再保业务现金净额',
        '保户储金及投资款净增加额', '收取利息、手续费及佣金的现金', '拆入资金净增加额',
        '回购业务资金净增加额', '代理买卖证券收到的现金净额', '收到的税费返还',
        '收到其他与经营活动有关的现金', '经营活动现金流入小计', '购买商品、接受劳务支付的现金', '客户贷款及垫款净增加额',
        '存放中央银行和同业款项净增加额', '支付原保险合同赔付款项的现金', '拆出资金净增加额',
        '支付利息、手续费及佣金的现金',
        '支付保单红利的现金', '支付给职工以及为职工支付的现金', '支付的各项税费', '支付其他与经营活动有关的现金',
        '经营活动现金流出小计', '经营活动产生的现金流量净额',
        '收回投资收到的现金', '取得投资收益收到的现金', '处置固定资产、无形资产和其他长期资产收回的现金净额',
        '处置子公司及其他营业单位收到的现金净额', '收到其他与投资活动有关的现金', '投资活动现金流入小计',
        '购建固定资产、无形资产和其他长期资产支付的现金', '投资支付的现金', '质押贷款净增加额',
        '取得子公司及其他营业单位支付的现金净额',
        '支付其他与投资活动有关的现金', '投资活动现金流出小计', '投资活动产生的现金流量净额',
        '吸收投资收到的现金', '子公司吸收少数股东投资收到的现金', '取得借款收到的现金', '收到其他与筹资活动有关的现金',
        '筹资活动现金流入小计', '偿还债务支付的现金', '分配股利、利润或偿付利息支付的现金',
        '子公司支付给少数股东的股利、利润',
        '支付其他与筹资活动有关的现金', '筹资活动现金流出小计', '筹资活动产生的现金流量净额',
        '汇率变动对现金及现金等价物的影响',
        '现金及现金等价物净增加额', '期初现金及现金等价物余额', '期末现金及现金等价物余额']
    for _l in list2:
        answer_dict[_l] = ''

    with open(file_name, 'r',encoding='utf-8') as file:

        lines = file.readlines()
        for line in lines:
            line = line.replace('\n', '')
            try:
                line_dict = json.loads(line)
                # 处理逻辑
            except json.JSONDecodeError as e:
                print(f"JSON 解析错误: {e} 在文件 {file_name} 的行: {line}")
                continue

            try:
                if line_dict['type'] not in ['页眉', '页脚']:
                    all_text = all_text + line_dict['inside']

                # 合并资产负债表
                text1, check1 = cut_all_text(check1,
                                             '(?:财务报表.{0,15}|1、)(?:合并资产负债表)$',
                                             '(?:母公司资产负债表)$',
                                             all_text, line_dict, text1)
                # 母公司资产负债表
                text2, check2 = cut_all_text(check2,
                                             '(?:负责人.{0,15}|2、)(?:母公司资产负债表)$',
                                             '(?:合并利润表)$',
                                             all_text, line_dict, text2)
                # 合并利润表
                text3, check3 = cut_all_text(check3,
                                             '(?:负责人.{0,15}|3、)(?:合并利润表)$',
                                             '(?:母公司利润表)$',
                                             all_text, line_dict, text3)
                # 母公司利润表
                text4, check4 = cut_all_text(check4,
                                             '(?:负责人.{0,15}|4、)(?:母公司利润表)$',
                                             '(?:合并现金流量表)$',
                                             all_text, line_dict, text4)
                # 合并现金流量表
                text5, check5 = cut_all_text(check5,
                                             '(?:负责人.{0,15}|5、)(?:合并现金流量表)$',
                                             '(?:母公司现金流量表)$',
                                             all_text, line_dict, text5)
                if re.search('(?:负责人.{0,15}|6、)(?:母公司现金流量表)$', all_text):
                    break
                # # 母公司现金流量表
                # text6, check6 = cut_all_text(check6,
                #                              '(?:负责人.{0,15}|6、)(?:母公司现金流量表)$',
                #                              '(?:合并所有者权益变动表)$',
                #                              all_text, line_dict, text6)
                # # 合并所有者权益变动表
                # text7, check7 = cut_all_text(check7,
                #                              '(?:负责人.{0,15}|7、)(?:合并所有者权益变动表)$',
                #                              '(?:母公司所有者权益变动表)$',
                #                              all_text, line_dict, text7)
                # # 母公司所有者权益变动表
                # text8, check8 = cut_all_text(check8,
                #                              '(?:负责人.{0,15}|8、)(?:母公司所有者权益变动表)$',
                #                              '(?:公司基本情况)$',
                #                              all_text, line_dict, text8)

            except:
                print(line_dict)
                pass


    cut1_len = len(text1.split('合并资产负债表')[0])
    cut2_len = len(text2.split('母公司资产负债表')[0])
    cut3_len = len(text3.split('合并利润表')[0])
    cut4_len = len(text4.split('母公司利润表')[0])
    cut5_len = len(text5.split('合并现金流量表')[0])
    # cut6_len = len(text6.split('母公司现金流量表')[0])
    # cut7_len = len(text7.split('合并所有者权益变动表')[0])
    # cut8_len = len(text8.split('母公司所有者权益变动表')[0])

    # def check_data(answer_dict, text_check, addwords, stop_re):
    #     text_list = text_check.split('\n')
    #     data = []
    #     check_len = 0
    #     for _t in text_list:
    #         if re.search("\['项目", _t) and not re.search("调整数", _t) and check_len == 0:
    #             check_len = len(eval(_t))
    #             print(f"项目行长度: {check_len}")
    #         if re.search('^[\[]', _t):
    #             try:
    #                 text_l = eval(_t)
    #                 # 清洗和处理项目名称
    #                 text_l[0] = re.sub(r'[一二三四五六七八九十]、|（[一二三四五六七八九十]）|\d\.|加：|减：|其中：|（元/股）', '', text_l[0]).split('（')[0]
    #                 if check_len == len(text_l) and re.search('[\u4e00-\u9fa5]', text_l[0]):
    #                     data.append(text_l)
    #             except Exception as e:
    #                 print(f"数据处理错误: {e} 在文本: {_t}")
    #         if data and re.search(stop_re, _t):
    #             break

    #     if data:
    #         df = pd.DataFrame(data[1:], columns=data[0])
    #         df.replace('', '无', inplace=True)
    #         print(f"提取的 DataFrame 列: {df.columns.tolist()}")
    #         if f'{year}{addwords}' in df.columns and '项目' in df.columns:
    #             df = df.drop_duplicates(subset='项目', keep='first')
    #             for key in answer_dict:
    #                 try:
    #                     match_answer = df[df['项目'] == key]
    #                     if not match_answer.empty and answer_dict[key] == '':
    #                         answer_dict[key] = match_answer[f'{year}{addwords}'].values[0]
    #                         print(f"匹配到 {key}: {answer_dict[key]}")
    #                 except Exception as e:
    #                     print(f"匹配数据错误: {e} 对于键: {key}")
    #     else:
    #         print("未提取到有效数据")
    #     return answer_dict
    def check_data(answer_dict, text_check, target_columns, stop_re):
        text_list = text_check.split('\n')
        data = []
        check_len = 0
        for _t in text_list:
            if re.search("\['项目", _t) and not re.search("调整数", _t) and check_len == 0:
                try:
                    check_len = len(eval(_t))
                    print(f"项目行长度设置为: {check_len}")
                except Exception as e:
                    print(f"项目行长度设置失败: {e} 在文本: {_t}")
                    continue
            if re.search('^[\[]', _t):
                try:
                    text_l = eval(_t)
                    # 清洗和处理项目名称
                    original_project = text_l[0]
                    text_l[0] = re.sub(r'[一二三四五六七八九十]、|（[一二三四五六七八九十]）|\d\.|加：|减：|其中：|（元/股）', '', text_l[0]).split('（')[0].replace('：', '')
                    print(f"原项目名称: {original_project} -> 清洗后: {text_l[0]}")
                    if check_len == len(text_l) and re.search('[\u4e00-\u9fa5]', text_l[0]):
                        data.append(text_l)
                except Exception as e:
                    print(f"数据处理错误: {e} 在文本: {_t}")
            if data and re.search(stop_re, _t):
                break

        if data:
            try:
                df = pd.DataFrame(data[1:], columns=data[0])
                df.replace('', '无', inplace=True)
                print(f"提取的 DataFrame 列: {df.columns.tolist()}")
            except Exception as e:
                print(f"创建 DataFrame 失败: {e}")
                return answer_dict

            if '项目' not in df.columns:
                print("DataFrame 中缺少 '项目' 列")
                return answer_dict

            if set(target_columns).issubset(df.columns):
                try:
                    df = df.drop_duplicates(subset='项目', keep='first')
                    print(f"DataFrame 去重后的行数: {len(df)}")
                except KeyError as e:
                    print(f"去重失败，原因: {e}")
                    return answer_dict
                except Exception as e:
                    print(f"去重时发生错误: {e}")
                    return answer_dict

                for key in answer_dict:
                    try:
                        match_answer = df[df['项目'] == key]
                        if not match_answer.empty and answer_dict[key] == '':
                            answer_dict[key] = match_answer[target_columns].values[0]
                            print(f"匹配到 {key}: {answer_dict[key]}")
                        else:
                            print(f"未匹配到 {key}")
                    except Exception as e:
                        print(f"匹配数据错误: {e} 对于键: {key}")
            else:
                print(f"缺少目标列: {target_columns}，实际列: {df.columns.tolist()}")
        else:
            print("未提取到有效数据")
        return answer_dict



    # answer_dict = check_data(answer_dict, text1[cut1_len:], '12月31日', '合并资产负债表')
    # answer_dict = check_data(answer_dict, text3[cut3_len:], '度', '合并利润表')
    # answer_dict = check_data(answer_dict, text5[cut5_len:], '度', '合并现金流量表')

    # 修改 check_data 函数调用，根据实际列名传递正确的 addwords
    # 对应不同的报表，传递不同的目标列名
    # answer_dict = check_data(answer_dict, text1[cut1_len:], ['期末余额', '期初余额'], '合并资产负债表')
    # answer_dict = check_data(answer_dict, text3[cut3_len:], ['本期发生额', '上期发生额'], '合并利润表')
    # answer_dict = check_data(answer_dict, text5[cut5_len:], ['本期发生额', '上期发生额'], '合并现金流量表')

    # new_row = {
    #     '文件名': allname,
    #     # '日期': date, 
    #     '公司名称': name, '股票代码': stock, 
    #     # '股票简称': short_name, 
    #     '年份': year, '类型': '年度报告',
    #     '合并资产负债表': text1[cut1_len:], '合并利润表': text3[cut3_len:], '合并现金流量表': text5[cut5_len:], '全文': str(lines)}
    # for key in answer_dict:
    #     new_row[key] = answer_dict[key]
    # print('结束 '+file_name)
    # return new_row
    answer_dict = check_data(answer_dict, text1[cut1_len:], ['期末余额', '期初余额'], '合并资产负债表')
    answer_dict = check_data(answer_dict, text3[cut3_len:], ['本期发生额', '上期发生额'], '合并利润表')
    answer_dict = check_data(answer_dict, text5[cut5_len:], ['本期发生额', '上期发生额'], '合并现金流量表')

    # 构建新的一行数据
    new_row = {
        '文件名': allname,
        '公司名称': name,
        '股票代码': stock,
        '年份': year,
        '类型': '年度报告',
        '合并资产负债表': text1[cut1_len:],
        '合并利润表': text3[cut3_len:],
        '合并现金流量表': text5[cut5_len:],
        '全文': str(lines)
    }
    for key in answer_dict:
        new_row[key] = answer_dict[key]

    print('结束 ' + file_name)
    return new_row

if __name__ == "__main__":
    # 文件夹路径
    # folder_path = '../alltxt'
    # 获取文件夹内所有文件名称
    # file_names = glob.glob(folder_path + '/*')
    # file_names = sorted(file_names, reverse=True)
    # file_names = file_names[0:100]
    folder_path = os.path.join('..', 'alltxt2')
    file_names = glob.glob(os.path.join(folder_path, '*'))
    file_names = sorted(file_names, reverse=True)

    print(file_names)
    results = []
    # 打印文件名称
    list1 = [
        '文件名', '日期', '公司名称', '股票代码', '股票简称', '年份', '类型',
        '合并资产负债表', '合并利润表', '合并利润表', '全文']
    list2 = [        '货币资金', '结算备付金', '拆出资金', '交易性金融资产', '以公允价值计量且其变动计入当期损益的金融资产', '衍生金融资产', '应收票据',
        '应收账款', '应收款项融资', '预付款项', '应收保费', '应收分保账款', '应收分保合同准备金', '其他应收款', '应收利息', '应收股利',
        '买入返售金融资产', '存货', '合同资产', '持有待售资产', '一年内到期的非流动资产', '其他流动资产',
        '流动资产合计',
        '发放贷款和垫款', '债权投资', '可供出售金融资产', '其他债权投资', '持有至到期投资', '长期应收款', '长期股权投资',
        '其他权益工具投资', '其他非流动金融资产', '投资性房地产', '固定资产', '在建工程', '生产性生物资产', '油气资产', '使用权资产',
        '无形资产', '开发支出', '商誉', '长期待摊费用', '递延所得税资产', '其他非流动资产',
        '非流动资产合计', '资产总计',
        '短期借款', '向中央银行借款', '拆入资金', '交易性金融负债', '以公允价值计量且其变动计入当期损益的金融负债', '衍生金融负债',
        '应付票据', '应付账款', '预收款项', '合同负债', '卖出回购金融资产款', '吸收存款及同业存放', '代理买卖证券款', '代理承销证券款',
        '应付职工薪酬', '应交税费', '其他应付款', '应付利息', '应付股利', '应付手续费及佣金', '应付分保账款', '持有待售负债',
        '一年内到期的非流动负债', '其他流动负债', '流动负债合计',
        '保险合同准备金', '长期借款', '应付债券', '租赁负债', '长期应付款', '长期应付职工薪酬', '预计负债', '递延收益', '递延所得税负债',
        '其他非流动负债', '非流动负债合计', '负债合计',
        '股本', '实收资本', '其他权益工具', '资本公积', '库存股', '其他综合收益', '专项储备', '盈余公积', '一般风险准备', '未分配利润',
        '归属于母公司所有者权益合计', '少数股东权益', '所有者权益合计', '负债和所有者权益总计',
        '营业总收入', '营业收入', '利息收入', '已赚保费', '手续费及佣金收入',
        '营业总成本', '营业成本', '利息支出', '手续费及佣金支出',
        '退保金', '赔付支出净额', '提取保险责任合同准备金净额', '保单红利支出', '分保费用', '税金及附加',
        '销售费用', '管理费用', '研发费用', '财务费用', '利息费用', '其他收益',
        '投资收益', '其中：对联营企业和合营企业的投资收益', '以摊余成本计量的金融资产终止确认收益', '汇兑收益', '净敞口套期收益',
        '公允价值变动收益', '信用减值损失', '资产减值损失', '资产处置收益',
        '营业利润', '营业外收入', '营业外支出', '利润总额', '所得税费用',
        '净利润', '按经营持续性分类', '持续经营净利润', '终止经营净利润', '按所有权归属分类', '归属于母公司所有者的净利润', '少数股东损益',
        '其他综合收益的税后净额', '归属母公司所有者的其他综合收益的税后净额', '不能重分类进损益的其他综合收益',
        '重新计量设定受益计划变动额', '权益法下不能转损益的其他综合收益', '其他权益工具投资公允价值变动', '企业自身信用风险公允价值变动', '其他',
        '将重分类进损益的其他综合收益', '权益法下可转损益的其他综合收益', '其他债权投资公允价值变动', '可供出售金融资产公允价值变动损益',
        '金融资产重分类计入其他综合收益的金额',  '持有至到期投资重分类为可供出售金融资产损益',
        '其他债权投资信用减值准备', '现金流量套期储备', '外币财务报表折算差额', '其他',
        '归属于少数股东的其他综合收益的税后净额',
        '综合收益总额', '归属于母公司所有者的综合收益总额', '归属于少数股东的综合收益总额',
        '基本每股收益', '稀释每股收益',
        '销售商品、提供劳务收到的现金', '客户存款和同业存放款项净增加额', '向中央银行借款净增加额',
        '向其他金融机构拆入资金净增加额', '收到原保险合同保费取得的现金', '收到再保业务现金净额',
        '保户储金及投资款净增加额', '收取利息、手续费及佣金的现金', '拆入资金净增加额',
        '回购业务资金净增加额', '代理买卖证券收到的现金净额', '收到的税费返还',
        '收到其他与经营活动有关的现金', '经营活动现金流入小计', '购买商品、接受劳务支付的现金', '客户贷款及垫款净增加额',
        '存放中央银行和同业款项净增加额', '支付原保险合同赔付款项的现金', '拆出资金净增加额', '支付利息、手续费及佣金的现金',
        '支付保单红利的现金', '支付给职工以及为职工支付的现金', '支付的各项税费', '支付其他与经营活动有关的现金',
        '经营活动现金流出小计', '经营活动产生的现金流量净额',
        '收回投资收到的现金', '取得投资收益收到的现金', '处置固定资产、无形资产和其他长期资产收回的现金净额',
        '处置子公司及其他营业单位收到的现金净额', '收到其他与投资活动有关的现金', '投资活动现金流入小计',
        '购建固定资产、无形资产和其他长期资产支付的现金', '投资支付的现金', '质押贷款净增加额', '取得子公司及其他营业单位支付的现金净额',
        '支付其他与投资活动有关的现金', '投资活动现金流出小计', '投资活动产生的现金流量净额',
        '吸收投资收到的现金', '子公司吸收少数股东投资收到的现金', '取得借款收到的现金', '收到其他与筹资活动有关的现金',
        '筹资活动现金流入小计', '偿还债务支付的现金', '分配股利、利润或偿付利息支付的现金', '子公司支付给少数股东的股利、利润',
        '支付其他与筹资活动有关的现金', '筹资活动现金流出小计', '筹资活动产生的现金流量净额',
        '汇率变动对现金及现金等价物的影响',
        '现金及现金等价物净增加额', '期初现金及现金等价物余额', '期末现金及现金等价物余额']

    all_list = list1+list2
    df = pd.DataFrame(columns=all_list)



    with Pool(processes=10) as pool:
        results = pool.map(process_file, file_names)

    df = pd.DataFrame(results)
    df.to_excel("big_data2.xlsx", index=False)

# if __name__ == "__main__":
#     test_file = '..\\alltxt2\\900953_2018_凯马B2018年年度报告.txt'
#     new_row = process_file(test_file)
#     print(new_row)


