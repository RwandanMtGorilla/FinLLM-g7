import pandas as pd
import os
import re
from tqdm import tqdm

# 读取Excel文件
file_path = 'merged_data.xlsx'
df = pd.read_excel(file_path)

# 为tqdm添加进度条显示
tqdm.pandas(desc="Processing Rows")

# 定义函数：处理文件名，提取文件名、股票代码和年份
def process_row(row):
    filename = row['文件名']
    
    # 检查 filename 是否为字符串，如果不是则跳过此行处理
    if not isinstance(filename, str):
        return row
    
    # 检查是否为路径格式
    if '\\' in filename or '/' in filename:
        # 提取文件名
        base_filename = os.path.basename(filename)
        
        # 提取股票代码（六位连续数字）和年份（四位连续数字）
        stock_code_match = re.search(r'(\d{6})', base_filename)
        year_match = re.search(r'(19|20\d{2})', base_filename)
        
        # 更新文件名、股票代码和年份
        row['文件名'] = base_filename
        if pd.isna(row['股票代码']) and stock_code_match:
            row['股票代码'] = stock_code_match.group(0)
        if pd.isna(row['年份']) and year_match:
            row['年份'] = year_match.group(0)
    
    # 去除其他列中数字的逗号
    for column in row.index:
        if isinstance(row[column], str) and re.match(r'^[\d,]+(\.\d+)?$', row[column]):
            row[column] = row[column].replace(',', '')
    
    return row

# 应用带进度条的函数处理每一行
df = df.progress_apply(process_row, axis=1)

# 将处理后的数据另存为新的Excel文件
output_file_path = 'merged_data_p.xlsx'
df.to_excel(output_file_path, index=False)

print(f"处理完成，结果已保存到 {output_file_path}")
