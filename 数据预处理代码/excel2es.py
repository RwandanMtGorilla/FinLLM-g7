import pandas as pd
from elasticsearch import Elasticsearch
from tqdm import tqdm

def clean_data(record, column_types):
    """
    清理数据，根据字段的类型选择性地转换数据。
    将数值字段中的非数值内容（如 '无', '不适用', '-', '—'）设为 None。
    对于字符串字段，保留原始内容，但将 '无', '不适用', '-' 和 '—' 转换为 None。
    同时去除数值字段中的逗号，并处理包含多个数值的情况。
    """
    for key, value in record.items():
        # 检查该字段的预期类型
        if column_types[key] in ['float64', 'int64']:
            # 数值字段，将 '无', '不适用', '-', '—' 和无法解析为数字的内容转换为 None
            if pd.isna(value) or value in ['无', '不适用', '-', '—']:
                record[key] = None
                print(f"Setting '{key}' to None due to invalid value: {value}")
            elif isinstance(value, str):
                # 处理多个数值的情况，只保留第一个数值
                value_parts = value.split()
                if len(value_parts) > 1:
                    print(f"Field '{key}' contains multiple values. Original value: '{value}' -> Keeping first value: '{value_parts[0]}'")
                    value = value_parts[0]  # 取第一个数值，忽略其他部分

                # 去掉数值中的逗号
                value = value.replace(',', '')
                try:
                    record[key] = float(value)
                    print(f"Converted field '{key}' to float: {record[key]}")
                except ValueError:
                    print(f"Failed to convert '{value}' to float in field '{key}'. Setting to None.")
                    record[key] = None
        elif column_types[key] == 'object':  # 字符串字段
            # 对于字符串字段，将 '无', '不适用', '-' 和 '—' 转换为 None
            if value in ['无', '不适用', '-', '—']:
                record[key] = None
                print(f"Setting string field '{key}' to None due to invalid value: {value}")
    return record

def excel2es(excel_path, es_host, index_name, batch_size=1000):
    # 读取 Excel 文件
    df = pd.read_excel(excel_path, engine='openpyxl')

    # 将所有 `NaN` 值替换为 `None`
    df = df.where(pd.notnull(df), None)

    # 获取每列的类型，用于后续的字段清理
    column_types = df.dtypes.apply(lambda x: x.name).to_dict()

    # 连接到 Elasticsearch
    es = Elasticsearch([es_host])

    # 如果索引存在，删除它
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # 创建新索引
    es.indices.create(index=index_name, ignore=400)

    # 将 DataFrame 转换为字典记录
    records = df.to_dict(orient='records')

    # 使用 tqdm 显示进度条
    for i in tqdm(range(0, len(records), batch_size), desc="Inserting records"):
        batch = records[i:i + batch_size]
        
        # 清理每条记录数据
        batch = [clean_data(record, column_types) for record in batch]
        
        for record in batch:
            # 插入记录到 Elasticsearch
            try:
                es.index(index=index_name, body=record)
            except Exception as e:
                print(f"Failed to insert record {record} due to error: {e}")
    
    print("All records inserted successfully.")

if __name__ == "__main__":
    excel_path = 'merged_data.xlsx'
    es_host = 'http://localhost:9200'
    index_name = 'testindex'
    batch_size = 1000  # 每次插入的记录数
    excel2es(excel_path, es_host, index_name, batch_size)
