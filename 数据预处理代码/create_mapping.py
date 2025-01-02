import os
import json
import pandas as pd

# 定义文件夹路径和目标 Excel 文件
folder_path = r"E:\download"
output_file = "security_code_mapping.xlsx"

# 创建一个集合来存储唯一的 (security_code, security_name) 元组
unique_mappings = set()

# 遍历文件夹中的所有文件
for filename in os.listdir(folder_path):
    # 检查是否是 .json 文件
    if filename.endswith(".json"):
        file_path = os.path.join(folder_path, filename)
        # 打开并读取 .json 文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 提取 security_code 和 security_name 并添加到集合中
            security_code = data.get("security_code")
            security_name = data.get("security_name")
            if security_code and security_name:
                unique_mappings.add((security_code, security_name))

# 将集合转化为 DataFrame 并导出到 Excel
df = pd.DataFrame(list(unique_mappings), columns=["security_code", "security_name"])
df.to_excel(output_file, index=False)

print(f"映射关系已保存至 {output_file}")
