import pandas as pd

# 定义读取Excel文件的函数
def read_excel_file(file_path):
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        print(f"成功读取文件: {file_path}")
        return df
    except Exception as e:
        print(f"读取文件 {file_path} 时出错: {e}")
        raise

# 读取四个Excel文件
df1 = read_excel_file("big_data1.xlsx")
df2 = read_excel_file("big_data2.xlsx")
df3 = read_excel_file("big_data3.xlsx")
df4 = read_excel_file("industry_.xlsx")

# 检查前三个文件中是否存在“文件名”列
required_column = "文件名"
for i, df in enumerate([df1, df2, df3], start=1):
    if required_column not in df.columns:
        raise ValueError(f"big_data{i}.xlsx 中缺少 '{required_column}' 列。")
    else:
        print(f"big_data{i}.xlsx 中包含 '{required_column}' 列。")

# 检查第四个文件中是否存在“股票代码”列
if "股票代码" not in df4.columns:
    raise ValueError("industry_.xlsx 中缺少 '股票代码' 列。")
else:
    print("industry_.xlsx 中包含 '股票代码' 列。")

# 合并前三个DataFrame，基于“文件名”列，使用内连接
merged_df = df1.merge(df2, on="文件名", how='inner').merge(df3, on="文件名", how='inner')

print("前三个DataFrame 已成功合并。")

# 合并第四个DataFrame，基于“股票代码”列，使用内连接
# 假设前三个DataFrame合并后的DataFrame中也包含“股票代码”列
# 如果“股票代码”列在前三个DataFrame中不存在，需要确保合并逻辑正确
if "股票代码" not in merged_df.columns:
    raise ValueError("合并后的前三个DataFrame 中缺少 '股票代码' 列，无法与 industry_.xlsx 合并。")
else:
    print("合并后的DataFrame 包含 '股票代码' 列，可以与 industry_.xlsx 合并。")

# 进行最终合并
final_merged_df = merged_df.merge(df4, on="股票代码", how='inner')

print("所有DataFrame 已成功合并。")

# 保存合并后的DataFrame到新的Excel文件
output_file = "big_data_merged.xlsx"
try:
    final_merged_df.to_excel(output_file, engine='openpyxl', index=False)
    print(f"合并后的数据已成功保存到 '{output_file}'。")
except Exception as e:
    print(f"保存文件 {output_file} 时出错: {e}")
    raise
