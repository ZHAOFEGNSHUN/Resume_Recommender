# coding:utf-8
from py2neo import Graph, Node, Relationship
from py2neo.data import walk, Subgraph
from tqdm import tqdm
import json

def batch_create(graph, nodes_list, relations_list):
    """
        批量创建节点/关系,nodes_list和relations_list不同时为空即可
        特别的：当利用关系创建节点时，可使得nodes_list=[]
    :param graph: Graph()
    :param nodes_list: Node()集合
    :param relations_list: Relationship集合
    :return:
    """

    subgraph = Subgraph(nodes_list, relations_list)
    tx_ = graph.begin()
    tx_.create(subgraph)
    graph.commit(tx_)

if __name__ == '__main__':
    # 连接neo4j
    graph = Graph("neo4j://localhost:7687", auth=("neo4j", "tiamo"))
    
    # 读取 JSON_JD 文件
    with open("/Users/bytedance/Desktop/ZFS/Resume_Recommender/merged_1700.json", "r") as json_file:
        data_job = json.load(json_file)
    # 读取 JSON_CV 文件
    with open("/Users/bytedance/Desktop/ZFS/Resume_Recommender/merged_resume.json", "r") as json_file:
        data_jd = json.load(json_file)

    i,j = 0,0
    id_skill_mapping = {}
    nodes_list = []
    # 循环遍历数据
for item in tqdm(data_job, desc="Inserting JD data"):
    jd_id = str(item['id'])
    node_JD = Node("岗位", name=f"岗位{jd_id}")
    nodes_list.append(node_JD)
    # 创建job和skill之间的关系
    relations_list = []
    for annotation in item['annotations']:
        for label_info in annotation['result']:
            if '技能' in label_info['value']['labels']:
                skill_name = label_info['value']['text']
                 # 检查技能是否已存在
                existing_skill_node = next((node for node in nodes_list if node['name'] == skill_name), None)

                if existing_skill_node is None:
                    # 技能不存在，创建新技能节点
                    node_Skill = Node("技能", name=f"{skill_name}")
                    nodes_list.append(node_Skill)

                    # 创建技能与岗位之间的关系
                    relation_Skill_Job = Relationship(node_JD, "需要", node_Skill)
                    relations_list.append(relation_Skill_Job)
                else:
                    # 技能已存在，使用已有技能节点
                    node_Skill = existing_skill_node

                    # 创建技能与简历之间的关系
                    relation_Skill_Resume = Relationship(node_JD, "需要", node_Skill)
                    relations_list.append(relation_Skill_Resume)
    # 使用批量创建函数
    batch_create(graph, nodes_list, relations_list)

for item in tqdm(data_jd, desc="Inserting CV data"):
    cv_id = str(item['id'])
    node_CV = Node("简历", name=f"简历{cv_id}")
    nodes_list.append(node_CV)
    # 创建job和skill之间的关系
    relations_list = []
    for annotation in item['annotations']:
        for label_info in annotation['result']:
            if '技能' in label_info['value']['labels']:
                skill_name = label_info['value']['text']
                 # 检查技能是否已存在
                existing_skill_node = next((node for node in nodes_list if node['name'] == skill_name), None)

                if existing_skill_node is None:
                    # 技能不存在，创建新技能节点
                    node_Skill = Node("技能", name=f"{skill_name}")
                    nodes_list.append(node_Skill)

                    # 创建技能与岗位之间的关系
                    relation_Skill_Job = Relationship(node_CV, "掌握", node_Skill)
                    relations_list.append(relation_Skill_Job)
                else:
                    # 技能已存在，使用已有技能节点
                    node_Skill = existing_skill_node

                    # 创建技能与简历之间的关系
                    relation_Skill_Resume = Relationship(node_CV, "掌握", node_Skill)
                    relations_list.append(relation_Skill_Resume)
    # 使用批量创建函数
    batch_create(graph, nodes_list, relations_list)


# 查询root节点相连的n度 子 节点,后继节点
def get_children(base, degree=1) -> list:
    """
    :param base: 出发的图节点
    :param degree: 度数，默认为一
    :return: 符合条件的node集合
    """
    target_list = []
    nodes = graph.run("MATCH (node)-[:contains*" + str(
        degree) + "]->(s_node) where node.name='" + base + "' return s_node.name").data()
    for i in nodes:
        target_list.append(i["s_node.name"])
        # print(i["s_node.name"])
    return target_list


# 查询root节点相连的n度 父亲 节点，前驱结点
def get_parent(base, degree=1) -> list:
    """
    :param base: 出发的图节点
    :param degree: 度数，默认为一
    :return: 符合条件的node集合
    """
    target_list = []
    nodes = graph.run("MATCH (node)<-[:contains*" + str(
        degree) + "]-(s_node) where node.name='" + base + "' return s_node.name").data()
    for i in nodes:
        target_list.append(i["s_node.name"])
        # print(i["s_node.name"])
    return target_list


# 查询兄弟节点（同一个父节点），同前驱节点
def get_bro(base) -> list:
    """
    :param base: 出发的图节点
    :return: 符合条件的node集合
    """
    target_list = []
    nodes = graph.run(
        "MATCH (s_node)-[:contains*1]->(node) where node.name='" + base + "' MATCH (s_node)-[:contains*1]->(b_node) where b_node.name <> node.name  return b_node.name").data()
    for i in nodes:
        target_list.append(i["b_node.name"])
        # print(i["s_node.name"])
    return target_list


# 查询兄弟节点（同一个爷爷节点），前驱的前驱节点相同
def get_cousin(base) -> list:
    """
    :param base: 出发的图节点
    :return: 符合条件的node集合
    """
    target_list = []
    nodes = graph.run(
        "MATCH (g_node)-[:contains*2]->(node) where node.name='" + base + "' MATCH (p_node)-[:contains*1]->(node) where node.name='" + base + "' MATCH (g_node)-[:contains*1]->(b_node)  where b_node.name <> p_node.name MATCH (b_node)-[:contains*1]->(c_node) return c_node.name").data()
    for i in nodes:
        target_list.append(i["c_node.name"])
        # print(i["s_node.name"])
    return target_list


# 是否为叶子节点
def is_leaf(base) -> bool:
    """
    :param base: 出发的图节点
    :return: 判断是否为叶子节点
    """
    nodes = graph.run(
        "MATCH (node)-[:contains*1]->(c_node) where node.name='" + base + "'return c_node.name").data()
    if nodes:
        return False
    return True


# 是否有孩子节点
def has_children(base):
    """
    :param base: 出发的图节点
    :return: 判断是否有孩子节点
    """
    nodes = graph.run(
        "MATCH (node)-[:contains*1]->(c_node) where node.name='" + base + "'return c_node.name").data()
    if nodes:
        return True
    return False


# 是否有兄弟节点
def has_bro(base):
    """
    :param base: 出发的图节点
    :return: 是否有兄弟节点
    """
    target_list = []
    nodes = graph.run(
        "MATCH (s_node)-[:contains*1]->(node) where node.name='" + base + "' MATCH (s_node)-[:contains*1]->(b_node) where b_node.name <> node.name  return b_node.name").data()
    for i in nodes:
        target_list.append(i["b_node.name"])
        # print(i["s_node.name"])
    return target_list is not None


# 是否为叶子节点的父节点，也就是说是不是倒数第二层节点
def is_p_leaf(base) -> bool:
    """
    :param base: 出发的图节点
    :return: 判断是否为叶子节点
    """
    nodes = graph.run(
        "MATCH (node)-[:contains*2]->(c_node) where node.name='" + base + "'return c_node.name").data()
    if nodes:
        return False
    return True


# 是否为语言相关节点
def is_language_node(base) -> bool:
    """
    :param base: 出发的图节点
    :return: 判断是否为叶子节点
    """
    if not get_parent(base):
        return False
    if get_parent(base)[0] == "language":
        return True
    return False


def cal_similarity(i, j):
    """
    similarity 计算
    :param i: dict of jd
    :param j: dict of cv
    :return:
    """
    jd_skill_dict = i["skill_pair"]
    jd_skill_set = set(jd_skill_dict.keys())

    # print(jd_skill_dict)
    length_of_jd = len(jd_skill_set)

    count = 0
    cv_skill_dict = j["skill_pair"]
    # print(cv_skill_dict)
    cv_skill_set = set(cv_skill_dict.keys())

    for k in jd_skill_set:
        # 为叶子节点
        if is_leaf(k):
            # cv 有相同节点
            if k in cv_skill_set:
                # 给完全匹配的语言节点高权重
                if is_language_node(k):
                    if jd_skill_dict[k] <= cv_skill_dict[k]:
                        count += 1 * 2
                    # 双方都有实体，且工作要求高于CV的掌握
                    elif jd_skill_dict[k] > cv_skill_dict[k]:
                        count += (3 - (jd_skill_dict[k] - cv_skill_dict[k])) / 3 * 2
                else:
                    if jd_skill_dict[k] <= cv_skill_dict[k]:
                        count += 1
                    # 双方都有实体，且工作要求高于CV的掌握
                    elif jd_skill_dict[k] > cv_skill_dict[k]:
                        count += (3 - (jd_skill_dict[k] - cv_skill_dict[k])) / 3

            else:
                # 有兄弟节点,如果cv中有一个兄弟节点就退出循环
                if has_bro(k):
                    flag = 0
                    for node in get_bro(k):
                        if node in cv_skill_set:
                            flag = 1
                            if jd_skill_dict[k] <= cv_skill_dict[node]:
                                count += 1 * 0.9
                            # 双方都有实体，且工作要求高于CV的掌握
                            elif jd_skill_dict[k] > cv_skill_dict[node]:
                                count += (3 - (jd_skill_dict[k] - cv_skill_dict[node])) / 3 * 0.9
                            break
                    if flag == 0:
                        count += 0
                else:
                    if get_parent(k)[0] in cv_skill_set:
                        node = get_parent(k)[0]
                        if jd_skill_dict[k] <= cv_skill_dict[node]:
                            count += 1 * 0.6
                        # 双方都有实体，且工作要求高于CV的掌握
                        elif jd_skill_dict[k] > cv_skill_dict[node]:
                            count += (3 - (jd_skill_dict[k] - cv_skill_dict[node])) / 3 * 0.6
        # 非叶节点
        else:
            if k in cv_skill_set:
                if jd_skill_dict[k] < cv_skill_dict[k]:
                    count += 1
                # 双方都有实体，且工作要求高于CV的掌握
                elif jd_skill_dict[k] > cv_skill_dict[k]:
                    count += (3 - (jd_skill_dict[k] - cv_skill_dict[k])) / 3

            else:
                if has_children(k):
                    flag = 0
                    for node in get_children(k):
                        if node in cv_skill_set:
                            flag = 1
                            if jd_skill_dict[k] <= cv_skill_dict[node]:
                                count += 1
                            # 双方都有实体，且工作要求高于CV的掌握
                            elif jd_skill_dict[k] > cv_skill_dict[node]:
                                count += (3 - (jd_skill_dict[k] - cv_skill_dict[node])) / 3
                            break
                    if flag == 0:
                        count += 0
                # 有兄弟节点,如果cv中有一个兄弟节点就退出循环
                elif has_bro(k):
                    flag = 0
                    for node in get_bro(k):
                        if node in cv_skill_set:
                            flag = 1
                            if jd_skill_dict[k] <= cv_skill_dict[node]:
                                count += 1 * 0.9
                            # 双方都有实体，且工作要求高于CV的掌握
                            elif jd_skill_dict[k] > cv_skill_dict[node]:
                                count += (3 - (jd_skill_dict[k] - cv_skill_dict[node])) / 3 * 0.9
                            break
                    if flag == 0:
                        count += 0
                else:
                    if get_parent(k)[0] in cv_skill_set:
                        node = get_parent(k)[0]
                        if jd_skill_dict[k] <= cv_skill_dict[node]:
                            count += 1 * 0.6
                        # 双方都有实体，且工作要求高于CV的掌握
                        elif jd_skill_dict[k] > cv_skill_dict[node]:
                            count += (3 - (jd_skill_dict[k] - cv_skill_dict[node])) / 3 * 0.6

    # print(count / length_of_jd)
    # print("==============================")
    return count / length_of_jd


# if __name__ == '__main__':
#     print(is_language_node("java"))
