import pickle as pk

# import os
# print(os.getcwd())

data = pk.load(file=open('./cv_1000_raw_id_backup.bin', 'rb'))
# print(data[1:2])
# print("===========")
clean_list = []

for i in data:
    each_dict = {}
    for k,v in i.items():
        each_dict[k] = v
    if not i["work_experience"]:
        each_dict["work_experience"] = "暂无工作经验"
    elif '工作经验' in i["work_experience"]:
        pass
    elif '年' in i["work_experience"]:
        each_dict["work_experience"] = i["work_experience"] + '工作经验'
    else:
        each_dict["work_experience"] = i["work_experience"] + '年工作经验'
    clean_list.append(each_dict)
print(len(clean_list))
for idx, entry in enumerate(clean_list[:2]):
    print(f"Entry {idx + 1}:")
    print(entry)
    print("\n" + "="*20 + "\n")  # 用分隔线隔开每一条结果

# pk.dump(clean_list, file=open('./cv_1000_raw_id.bin', 'wb'))