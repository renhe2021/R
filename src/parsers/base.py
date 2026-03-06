"""解析器抽象基类"""

from abc import ABC, abstractmethod
from ..models import BookContent


class BaseParser(ABC):
    @abstractmethod
    def parse(self, file_path: str) -> BookContent:
        """解析文件，返回 BookContent"""
        ...

    @abstractmethod
    def supports(self, file_path: str) -> bool:
        """是否支持该文件格式"""
        ...
