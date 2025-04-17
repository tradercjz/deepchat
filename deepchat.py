
from typing import List, Any,Callable
from llm_prompt import llm 
import json
import concurrent.futures
import pandas as pd
from dotenv import load_dotenv
import os
from ddb import DatabaseSession

from logger import ThreadSafeLogger

load_dotenv()

@llm.prompt()
def summarize(conversation: List[dict]) -> str:
    """
    你是一个专业的对话分析师，你需要根据下面的对话历史，分析出所讨论的所有问题、以及针对每个问题的详细讨论过程和最终解决方案。
    在分析时，你需要关注对话的上下文和细节，一个问题可能会在多个部分被提及和讨论，并且期间可能穿插其他问题的讨论，你需要把同一个问题的所有相关讨论内容都汇总起来。
    如果问题是同一个，但是有不同的人提出了相似问题，也算同一个问题。
    你需要准确地识别出每个问题的提出者、详细的讨论过程、以及最终是否得到解决，以及是谁提供的解决方案。
    当无法从上下文中找到问题的明确解决方案时，请将解决方案标记为“未解决”。

    下面是对话历史：
    {% for conv in conversation %}
      {{ loop.index }}. {{ conv.msgtime }} - {{ conv.member_name }}({{ conv.sender }}): {{ conv.content }}
    {% endfor %}

    你需要提供如下格式的输出，要求输出严格按照该格式，不要有任何改动：
    [{
        "category": "问题分类1", 
        "questions": [{
            "Q": "具体的问题内容1",
            "asker": "问题的提出者",
            "discussion": ["关于问题1的讨论内容1","关于问题1的讨论内容2"...],
            "A": "对应的解决办法1 或 未解决",
            "solver": "解答人，如果没有解决写未解决",
            "T": ["问题出现的的时间1","问题出现的的时间2"...],
            
        },
       {
            "Q": "具体的问题内容2",
            "asker": "问题的提出者",
            "discussion": ["关于问题2的讨论内容1","关于问题2的讨论内容2"...],
            "A": "对应的解决办法2 或 未解决",
            "solver": "解答人，如果没有解决写未解决",
            "T": ["问题出现的的时间1","问题出现的的时间2"...],
        }]
    }
    ]


    输出的时候，不要有 json包裹，直接从 [{开始输出。输出}]之后，就结束，后续不要任何输出。
    
    """
    return {"conversation": conversation, "max_tokens": 4096}


def process_group_data(group_df: pd.DataFrame) -> Any:
    """
    处理单个分组的数据
    
    Args:
        group_df: 按group_name分组后的DataFrame子集
        
    Returns:
        处理结果
    """
    group_name = group_df['group_name'].iloc[0]
    logger = ThreadSafeLogger.get_logger(group_name.replace("/", "-"))
    json_str = group_df.to_json(orient='records', indent=4, force_ascii=False)

    try:
        json_obj = json.loads(json_str)  # type: ignore
        result = summarize(json_obj)

        logger.info(result)
        # TODO: 在这里实现你的实际处理逻辑
        # 示例：只是返回该组的消息数量
        return {
            'group_name': group_name,
            'message_count': len(group_df),
            'unique_senders': group_df['sender'].nunique()
        }
    
    except json.JSONDecodeError as e:
        print(e)
    except Exception as e:
        print(e)

def parallel_process_by_group(df: pd.DataFrame, 
                             process_func: Callable[[pd.DataFrame], Any],
                             max_workers: int = None) -> List[Any]:
    """
    按group_name分组并行处理DataFrame
    
    Args:
        df: 包含消息数据的DataFrame
        process_func: 处理单个分组数据的函数
        max_workers: 最大并行工作线程数，默认为None(由系统决定)
        
    Returns:
        所有分组处理结果的列表
    """
    # 按group_name分组
    grouped = df.groupby('group_name')
    
    # 创建一个线程池执行器
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 为每个分组提交一个处理任务
        future_to_group = {
            executor.submit(process_func, group_data): group_name 
            for group_name, group_data in grouped
        }
        
        # 收集所有结果
        results = []
        for future in concurrent.futures.as_completed(future_to_group):
            group_name = future_to_group[future]
            try:
                result = future.result()
                results.append(result)
                print(f"组 '{group_name}' 处理完成")
            except Exception as exc:
                print(f"处理组 '{group_name}' 时发生错误: {exc}")
                
        return results


if __name__ == "__main__":
    DDB_CONFIG = {
        "host": os.getenv("DDB_HOST", "127.0.0.1"),
        "port": os.getenv("DDB_PORT", "8848"),
        "user": os.getenv("DDB_USER", "admin"),
        "passwd": os.getenv("DDB_PASSWD", "123456")
    }

    script="""
    data=select * from loadTable("dfs://wecom","message")
    roomInfo = select * from loadTable("dfs://wecom","groupInfo")
    memberInfo = select * from loadTable("dfs://wecom","memberInfo")
    xdata = lj(data, roomInfo, ["roomid"])
    xdata2 = lj(xdata, memberInfo, ["sender"], ["memberid"])
    
    endDate = weekBegin(date(now()))
    startDate = temporalAdd(endDate, -7, 'd')
    toParseData = select msgid, sender, msgtime, roomid, string(content) as content, group_name, department, member_name, type from xdata2 where msgtime < endDate and msgtime >= startDate context by group_name order by msgtime asc
    toParseData
    """

    with DatabaseSession(**DDB_CONFIG) as db:
        success, result = db.execute(script)

        df = result
        print(f"原始DataFrame包含 {len(df)} 行数据，{df['group_name'].nunique()} 个不同的组")
        
        # 并行处理各组数据
        results = parallel_process_by_group(df, process_group_data, max_workers = 10)
        
        # 显示处理结果
        print("\n处理结果:")
        for result in results:
            print(f"组 '{result['group_name']}': {result['message_count']} 条消息, {result['unique_senders']} 个发送者")