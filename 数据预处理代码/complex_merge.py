import pandas as pd

# 读取数据1和数据2
data1_path = 'old_big_data.xlsx'  # 替换为数据1的文件路径
data2_path = 'cleaned_big_data.xlsx'  # 替换为数据2的文件路径

data1 = pd.read_excel(data1_path)
data2 = pd.read_excel(data2_path)

# 为了便于合并，重命名列以便于比较
data1.rename(columns={'证券代码': '股票代码_x', '年份': '年份_x'}, inplace=True)

# 定义需要重命名的列对照表
rename_dict = {
    '职工总人数': '职工总数',
    '销售人员人数': '销售人员',
    '技术人员人数': '技术人员',
    '硕士员工人数': '硕士人员',
    '研发人员人数': '研发人数',
    '博士及以上的员工人数': '博士及以上人员'
}

# 处理冲突列（名称不同但含义相同）
for col1, col2 in rename_dict.items():
    if col1 in data1.columns and col2 in data2.columns:
        # 优先选择 data2 中的列名
        data1.rename(columns={col1: col2}, inplace=True)

# 计数不同的字段
conflict_count = 0

# 创建一个新的 DataFrame 来存储合并后的数据
merged_data = data1.copy()

# 遍历数据2，检查每一行是否在数据1中存在
for index, row in data2.iterrows():
    # 通过股票代码和年份定位到数据1中的行
    match = data1[(data1['股票代码_x'] == row['股票代码_x']) & (data1['年份_x'] == row['年份_x'])]
    
    if not match.empty:
        # 如果找到了匹配的行
        match_index = match.index[0]
        
        # 检查所有列并进行合并
        for column in row.index:
            # 处理重复列：'文件名_x', '文件名_y', '年份_x', '年份_y' 等
            if column.endswith('_x') or column.endswith('_y'):
                base_column = column.rstrip('_x').rstrip('_y')  # 去除后缀得到基础列名
                
                # 确定保留不带后缀的列
                if base_column in data1.columns and base_column in data2.columns:
                    if pd.isna(merged_data.at[match_index, base_column]) or merged_data.at[match_index, base_column] in ['无', 'none']:
                        merged_data.at[match_index, base_column] = row[column]
                    elif pd.isna(row[column]) or row[column] in ['无', 'none']:
                        merged_data.at[match_index, base_column] = merged_data.at[match_index, base_column]
                    elif merged_data.at[match_index, base_column] != row[column]:
                        merged_data.at[match_index, base_column] = row[column]
                        conflict_count += 1
                else:
                    merged_data.at[match_index, column] = row[column]
            
            # 处理其他字段，优先选择数据2中的值
            elif column in data2.columns:
                if pd.isna(merged_data.at[match_index, column]) or merged_data.at[match_index, column] in ['无', 'none']:
                    merged_data.at[match_index, column] = row[column]
                elif merged_data.at[match_index, column] != row[column]:
                    merged_data.at[match_index, column] = row[column]
                    conflict_count += 1

    else:
        # 如果没有找到匹配的行，将数据2中的行添加到合并结果中
        merged_data = pd.concat([merged_data, row.to_frame().T], ignore_index=True)

# 删除在 data2 中没有匹配的 data1 行
merged_data = merged_data[merged_data['股票代码_x'].isin(data2['股票代码_x']) & merged_data['年份_x'].isin(data2['年份_x'])]

# 打印冲突计数
print(f"处理完成，共有 {conflict_count} 个字段值存在冲突，使用数据2中的值。")

# 删除重复的列（带后缀的列）
columns_to_drop = [col for col in merged_data.columns if '_x' in col or '_y' in col]
merged_data.drop(columns=columns_to_drop, inplace=True)

# 将合并后的数据保存到新的 Excel 文件
merged_output_path = 'merged_data.xlsx'  # 替换为你希望保存的文件路径
merged_data.to_excel(merged_output_path, index=False)

print("合并操作完成，结果已保存到", merged_output_path)
