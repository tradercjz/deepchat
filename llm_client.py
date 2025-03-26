from dataclasses import dataclass
from openai import OpenAI
from typing import List, Dict, Any
import os


@dataclass 
class LLMResponse:
    """通用LLM响应结果容器"""
    success: bool
    content: str = ""  # 原始响应内容
    reasoning_content: str = ""  # 推理内容
    error_message: str = ""
    error_type: str = ""
    metadata: Dict[str, Any] = None  # 可能的元数据

class LLMClient:
    """LLM客户端，处理与OpenAI API的交互"""
    
    def __init__(self, api_key: str = None, base_url: str = None, logger=None):
        """初始化LLM客户端
        
        Args:
            api_key: API密钥，默认从环境变量获取
            base_url: API基础URL，默认从环境变量获取
            logger: 日志记录器
        """
        self.client = OpenAI(
            api_key=api_key or os.getenv("DEEPSEEK_API_KEY"), 
            base_url=base_url or os.getenv("DEEPSEEK_URL")
        )
        self.logger = logger
    
    def generate_response(self, conversation_history: List[Dict[str, str]]) -> LLMResponse:
        """从LLM获取响应
        
        Args:
            conversation_history: 对话历史
            
        Returns:
            LLMResponse: llm原始返回
        """
        try:
            stream = self.client.chat.completions.create(
                model=os.getenv("DEEPSEEK_MODEL"),
                messages=conversation_history,
                max_completion_tokens=8000,
                stream=True
            )

            if self.logger:
                self.logger.info("Thinking...")
            
            reasoning_started = False
            reasoning_content = ""
            final_content = ""

            for chunk in stream:
                delta = chunk.choices[0].delta

                if hasattr(delta, 'reasoning_content') and delta.reasoning_content:
                    if self.logger and not reasoning_started:
                        self.logger.info("Reasoning:")
                        reasoning_started = True
                    reasoning_content += delta.reasoning_content
                elif hasattr(delta, 'content') and delta.content:
                    final_content += delta.content

            if self.logger:
                self.logger.info(f"Assistant> {final_content}")

             # 返回成功的响应对象，包含原始内容
            return LLMResponse(
                success=True,
                content=final_content,
                reasoning_content=reasoning_content,
                metadata={"model": os.getenv("DEEPSEEK_MODEL")}
            )

        except Exception as e:
            error_msg = f"DeepSeek API error: {str(e)}"
            if self.logger:
                self.logger.error(error_msg)
            return LLMResponse(
                success=False,
                error_message=error_msg,
                error_type=type(e).__name__
            )