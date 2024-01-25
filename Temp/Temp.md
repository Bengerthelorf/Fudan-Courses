```python
# 公司类别与薪资水平关系
# 计算各类别公司的平均薪资
# 新建一个空 DataFrame 用于储存公司类别及其平均薪资
df_region_salary = pd.DataFrame(columns=['公司类型', 'count', 'mean'])
df_region_salary['公司类别'] = df['公司类别'].unique()
df_region_salary = df_region_salary.set_index('公司类别')
df_region_salary['mean'] = 0
df_region_salary['count'] = 0

# 遍历 df_meaningful_salary 的数据, 检查其行业类型, 在相应count中+1, 并将其 Logarithmic mean 与 df_region_salary 中的 mean 相对 count 求均值
for index, row in df_meaningful_salary.iterrows():
    df_region_salary.at[row['公司类别'], 'count'] += 1
    df_region_salary.at[row['公司类别'], 'mean'] = (df_region_salary.at[row['公司类别'], 'mean'] * (df_region_salary.at[row['公司类别'], 'count'] - 1) + row['Logarithmic mean']) / df_region_salary.at[row['公司类别'], 'count']

# 处理 df_region_salary, 实现四舍五入, 并转换为整数形式, 并按照薪资大小进行排序
for index, row in df_region_salary.iterrows():
    df_region_salary.at[index, 'mean'] = round(df_region_salary.at[index, 'mean'])
    df_region_salary.at[index, 'mean'] = int(df_region_salary.at[index, 'mean'])

# 按照薪资大小进行排序
df_region_salary = df_region_salary.sort_values(by='mean', ascending=False)

# 画出直方图
plt.figure(figsize=(20, 6))
sns.barplot(x=df_region_salary.index, y=df_region_salary['mean'])
plt.xlabel('Company type')
plt.ylabel('Logarithmic mean')
plt.title('Logarithmic mean of each region')
plt.show()

df_region_salary
```