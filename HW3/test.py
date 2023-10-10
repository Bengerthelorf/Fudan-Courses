import enchant
import pandas as pd

df_software_skill = pd.read_excel('./')

print(df_software_skill)

d = enchant.DictWithPWL("en_US", "my_dict.txt")
for index, row in df_software_skill.iterrows():
    print(index)
    print(row['描述'])
    words = []
    for word in row['描述'].split():
        if d.check(word):
            print(word)
            words.append(word)
        else:
            suggestions = d.suggest(word)
            words.append(suggestions[0])
    if words != []:
        replacement = ' '.join(words)
        df_software_skill.at[index, '描述'] = df_software_skill.at[index, '描述'].replace(r'\b\w+\b', replacement)