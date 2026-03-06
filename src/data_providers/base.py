"""数据源抽象基类"""

import logging
from abc import ABC, abstractmethod
from ..analyzer import StockData

logger = logging.getLogger(__name__)


class DataProvider(ABC):
    """股票数据提供者的抽象接口"""

    @abstractmethod
    def fetch(self, symbol: str) -> StockData:
        """获取单只股票的完整数据"""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """检查数据源是否可用"""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称"""
        ...

    def fetch_with_validation(self, symbol: str) -> StockData:
        """获取数据并进行完整性验证

        Returns:
            StockData: 带有数据质量信息的股票数据
            stock._data_quality 包含:
                - coverage: data_coverage() 的结果
                - is_valid: 基本有效性
                - warnings: 警告列表
        """
        stock = self.fetch(symbol)

        warnings = []
        coverage = stock.data_coverage()

        # 检查基本有效性
        if not stock.is_valid():
            if stock.price is None or stock.price <= 0:
                warnings.append(f"[{self.name}] 未获取到股价")
            if not stock.name or stock.name == stock.symbol:
                warnings.append(f"[{self.name}] 未获取到公司名称")

        # 检查核心数据覆盖
        if coverage["core"]["pct"] < 50:
            warnings.append(f"[{self.name}] 核心数据覆盖率仅 {coverage['core']['pct']}% ({coverage['core']['filled']}/{coverage['core']['total']})")

        if coverage["missing_core"]:
            warnings.append(f"[{self.name}] 缺失核心字段: {', '.join(coverage['missing_core'])}")

        # 记录质量信息
        stock._data_quality = {
            "source": self.name,
            "coverage": coverage,
            "is_valid": stock.is_valid(),
            "warnings": warnings,
        }

        for w in warnings:
            logger.warning(w)

        if coverage["core"]["pct"] >= 75:
            logger.info(f"[{self.name}] {symbol} 数据质量良好: 核心覆盖 {coverage['core']['pct']}%")
        elif coverage["core"]["pct"] >= 50:
            logger.info(f"[{self.name}] {symbol} 数据质量一般: 核心覆盖 {coverage['core']['pct']}%")
        else:
            logger.warning(f"[{self.name}] {symbol} 数据质量差: 核心覆盖 {coverage['core']['pct']}%")

        return stock
