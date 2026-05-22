"""
Tab 2：持仓结构 —— "我持有什么，仓位是否合理"
Treemap 总览 -> 持仓明细表 -> 细分分类资产占比
"""

import streamlit as st
import pandas as pd
from opencc import OpenCC

from dashboard.utils.data_loader import (
    load_classified_holdings,
    load_penetration_summaries,
)
from dashboard.utils.chart_helper import (
    create_treemap,
    create_horizontal_stacked_bar,
    ASSET_COLORS,
)

# 繁体→简体转换器
_cc = OpenCC('t2s')


def _to_simplified(text: str) -> str:
    """繁体转简体"""
    return _cc.convert(text)


def _get_effective_class(row) -> str:
    """
    根据 level2_allocation 穿透比例确定实际资产大类
    取占比最高的那个类别，解决 fund 类统一着色的问题
    """
    alloc = row.get("level2_allocation") if "level2_allocation" in row.index else None
    if not isinstance(alloc, dict):
        # 没有穿透数据，回退到 asset_class
        return row.get("asset_class", "other")
    
    # 映射 pct 字段到分类名
    mapping = {
        "equity_pct": "equity",
        "bond_pct": "bond",
        "commodity_pct": "commodity",
        "cash_pct": "cash",
        "other_pct": "other",
    }
    
    max_pct = 0.0
    max_class = row.get("asset_class", "other")
    for pct_key, class_name in mapping.items():
        pct_val = alloc.get(pct_key, 0.0)
        if pct_val > max_pct:
            max_pct = pct_val
            max_class = class_name
    
    return max_class


def render():
    """渲染持仓结构页面"""
    # 加载数据
    df = load_classified_holdings()
    summaries = load_penetration_summaries()
    
    if df.empty:
        st.warning("未找到 classified_holdings 数据")
        return
    
    # ===== 区块 1：持仓全景 Treemap =====
    st.markdown("### 持仓全景")
    st.caption("面积代表市值占比，颜色代表穿透后资产大类（深蓝=股票，绿=债券，黄=商品，橙=现金）")
    
    # 对 asset_class 做映射，让 treemap 显示更友好
    df_treemap = df[df["market_value_cny"] > 0].copy()
    
    if not df_treemap.empty:
        # 基于 level2_allocation 穿透比例来确定实际资产大类（取占比最高的）
        df_treemap["asset_class"] = df_treemap.apply(_get_effective_class, axis=1)
        # 将繁体名称转为简体
        df_treemap["name"] = df_treemap["name"].apply(_to_simplified)
        fig_tree = create_treemap(df_treemap, "底层资产穿透")
        st.plotly_chart(fig_tree, use_container_width=True)
    else:
        st.info("暂无持仓数据")
    
    st.divider()
    
    # ===== 区块 2：持仓明细表 =====
    st.markdown("### 持仓明细")
    
    # 内嵌控件
    col1, col2 = st.columns(2)
    with col1:
        # 类别筛选
        all_classes = ["全部"] + sorted(df["asset_class"].unique().tolist())
        selected_class = st.selectbox("筛选类别", all_classes, key="holdings_filter")
    with col2:
        # 排序方式
        sort_options = {"市值 (高→低)": ("market_value_cny", False), 
                       "市值 (低→高)": ("market_value_cny", True),
                       "名称": ("name", True)}
        sort_choice = st.selectbox("排序方式", list(sort_options.keys()), key="holdings_sort")
    
    # 筛选
    df_display = df.copy()
    # 繁体转简体
    df_display["name"] = df_display["name"].apply(_to_simplified)
    if selected_class != "全部":
        df_display = df_display[df_display["asset_class"] == selected_class]
    
    # 排序
    sort_col, ascending = sort_options[sort_choice]
    df_display = df_display.sort_values(sort_col, ascending=ascending)
    
    # 计算占比
    total_mv = df["market_value_cny"].sum()
    df_display["占比"] = df_display["market_value_cny"].apply(
        lambda x: f"{x / total_mv * 100:.2f}%" if total_mv > 0 else "0%"
    )
    
    # 选择展示列
    display_cols = ["name", "code", "platform", "market_value_cny", "占比", "asset_class", "currency"]
    display_cols = [c for c in display_cols if c in df_display.columns]
    
    # 重命名列头
    col_rename = {
        "name": "名称",
        "code": "代码",
        "platform": "平台",
        "market_value_cny": "市值(CNY)",
        "asset_class": "类别",
        "currency": "币种",
    }
    
    st.dataframe(
        df_display[display_cols].rename(columns=col_rename),
        use_container_width=True,
        hide_index=True,
        height=400,
    )
    
    st.divider()
    
    # ===== 区块 3：细分分类资产占比 =====
    st.markdown("### 细分分类资产占比")
    st.caption("ETF / 个股 / 场外基金 / 货币基金 / 债基 等占比")
    
    # 从 penetration_summary 的 level1_summary 聚合
    level1_agg = {}
    for platform, data in summaries.items():
        level1 = data.get("level1_summary", {})
        for category, info in level1.items():
            mv = info.get("market_value_cny", 0)
            level1_agg[category] = level1_agg.get(category, 0) + mv
    
    if level1_agg:
        # 用水平条形图展示
        import plotly.graph_objects as go
        from dashboard.utils.chart_helper import LIGHT_LAYOUT
        
        sorted_items = sorted(level1_agg.items(), key=lambda x: -x[1])
        categories = [item[0] for item in sorted_items]
        values = [item[1] for item in sorted_items]
        total = sum(values)
        pct_labels = [f"{v/total*100:.1f}%" for v in values]
        
        fig = go.Figure(data=[go.Bar(
            y=categories,
            x=values,
            orientation="h",
            marker_color="#4ECDC4",
            text=pct_labels,
            textposition="outside",
            textfont=dict(color="#333333"),
        )])
        fig.update_layout(
            **LIGHT_LAYOUT,
            height=max(200, len(categories) * 50 + 80),
            xaxis_title="市值 (CNY)",
            xaxis=dict(
                tickfont=dict(color="#333333"),
                title=dict(text="市值 (CNY)", font=dict(color="#333333")),
                tickformat=",",
            ),
            yaxis=dict(
                tickfont=dict(color="#333333"),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无细分分类数据")
