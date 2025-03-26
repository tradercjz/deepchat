import functools
import inspect
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union
from jinja2 import Environment, BaseLoader
from llm_client import LLMClient,LLMResponse
from dotenv import load_dotenv


load_dotenv()

T = TypeVar('T')

class PromptDecorator:
    """
    一个类似于 @llm.prompt() 的装饰器，用于管理LLM提示模板
    """
    def __init__(self, 
                 response_model: Optional[Type] = None, 
                 stream: bool = False,
                 **kwargs):
        """
        初始化装饰器
        
        Args:
            response_model: 响应的数据模型类型
            stream: 是否启用流式响应
            **kwargs: 其他配置参数
        """
        self.response_model = response_model
        self.stream = stream
        self.kwargs = kwargs
        self.jinja_env = Environment(loader=BaseLoader())
        self.llm_client = LLMClient()
        
    def __call__(self, func: Callable[..., Dict[str, Any]]) -> Callable:
        """
        应用装饰器到函数
        
        Args:
            func: 被装饰的函数
        
        Returns:
            包装后的函数
        """
        # 获取函数的文档字符串作为模板
        docstring = inspect.getdoc(func)
        if not docstring:
            raise ValueError(f"函数 {func.__name__} 缺少文档字符串作为提示模板")
        
        # 预编译模板
        template = self.jinja_env.from_string(docstring)

         # 提取模板中的变量
        template_variables = self._extract_variables(docstring)
        
        # 获取函数签名
        sig = inspect.signature(func)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # 调用原函数
            result = func(*args, **kwargs)
            
            # 准备模板变量
            template_vars = {}
            
            # 如果函数返回字典，则使用其填充模板变量
            if isinstance(result, dict):
                template_vars.update(result)
            
            # 检查是否还有未填充的模板变量
            missing_vars = [var for var in template_variables if var not in template_vars]
            
            if missing_vars:
                # 尝试从函数参数中获取变量
                # 首先绑定参数
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                
                # 将参数添加到模板变量中
                for var in missing_vars:
                    if var in bound_args.arguments:
                        template_vars[var] = bound_args.arguments[var]
            
            # 检查是否仍有未填充的变量
            missing_vars = [var for var in template_variables if var not in template_vars]
            if missing_vars:
                raise ValueError(f"无法填充模板变量: {', '.join(missing_vars)}")
            
            # 渲染模板
            rendered_prompt = template.render(**template_vars)
            
            # 调用LLM API
            llm_result = self._call_llm_api(rendered_prompt)
            
            # 如果指定了响应模型，则进行类型转换
            if self.response_model:
                return self._convert_to_model(llm_result)
            
            # 如果原函数返回非字典，且LLM结果可以转换为相同类型，则尝试转换
            if not isinstance(result, dict):
                try:
                    return type(result)(llm_result)
                except (TypeError, ValueError):
                    pass
            
            return llm_result
        
        # 将原始模板和其他元数据附加到包装函数
        wrapper.prompt_template = docstring
        wrapper.response_model = self.response_model
        wrapper.stream = self.stream
        wrapper.template_variables = template_variables
        
        # 添加调试辅助方法
        def example_input():
            """返回使用示例输入的模板渲染结果"""
            example_vars = {k: f"example_{k}" for k in template_variables}
            return template.render(**example_vars)
        
        wrapper.example_input = example_input
        
        return wrapper
    
    def _call_llm_api(self, prompt: str) -> str:
        """
        调用LLM API (这里是一个模拟实现)
        
        Args:
            prompt: 渲染后的提示文本
            
        Returns:
            LLM的响应文本
        """
        # 在实际应用中，这里会调用OpenAI、Claude等LLM API
        #print(f"\n--- 发送到LLM的提示 ---\n{prompt}\n-----------------------")

        response = self.llm_client.generate_response([{
            "role": "user", 
            "content": prompt
        }])

        if response.success:
            return response.content
        else:
            return response.error_message
    
    def _convert_to_model(self, text: str) -> Any:
        """
        将文本转换为指定的响应模型
        
        Args:
            text: LLM返回的文本
            
        Returns:
            转换后的模型实例
        """
        if not self.response_model:
            return text
        
        # 如果是基本类型，直接转换
        if self.response_model in (str, int, float, bool):
            return self.response_model(text)
        
        # 对于复杂类型，可能需要解析JSON等
        # 这里简化处理，实际应用中可能需要更复杂的逻辑
        try:
            return self.response_model(text)
        except Exception as e:
            raise ValueError(f"无法将响应转换为 {self.response_model.__name__}: {e}")
    
    def _extract_variables(self, template_text: str) -> list:
        """
        从模板中提取变量名
        
        Args:
            template_text: 模板文本
            
        Returns:
            变量名列表
        """
        # 简单实现，实际应用可能需要更复杂的解析
        import re
        pattern = r"{{\s*(\w+)\s*}}"
        return re.findall(pattern, template_text)


class LLM:
    def prompt(self, *args, **kwargs):
        return PromptDecorator(*args, **kwargs)

llm = LLM()
