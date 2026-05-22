"""
Tab 1：概览 —— "我的资产全景"
一眼看清全貌：KPI 指标卡 + 资产大类配比 + 平台分布 + 币种敞口
"""

import streamlit as st

from dashboard.utils.data_loader import (
    load_penetration_summaries,
    load_indicator_results,
    get_total_market_value,
    get_level2_aggregated,
    get_currency_aggregated,
    get_platform_breakdown,
    get_portfolio_metrics,
)
from dashboard.utils.chart_helper import (
    create_donut_chart,
    create_horizontal_stacked_bar,
    create_currency_donut,
)


def render():
    """渲染概览页面"""
    # 加载数据
    summaries = load_penetration_summaries()
    indicator_data = load_indicator_results()
    
    if not summaries:
        st.warning("未找到 penetration_summary 数据，请先运行数据处理流程")
        return
    
    # ===== 隐藏金额开关 =====
    hide_amounts = st.toggle("🔒 隐藏金额", value=False, key="hide_amounts_toggle")
    
    # ===== 区块 1：KPI 指标卡 =====
    total_mv = get_total_market_value(summaries)
    portfolio_1y = get_portfolio_metrics(indicator_data, "1Y")
    portfolio_1m = get_portfolio_metrics(indicator_data, "1M")
    
    st.markdown("### 核心指标")
    
    # 用 container 包裹，通过 CSS 给容器加统一背景
    with st.container():
        st.markdown(
            """<style>
            [data-testid="stVerticalBlock"] > div:has(> [data-testid="stMetric"]) {
                background-color: #F8F9FA;
                border-radius: 12px;
                padding: 16px;
            }
            </style>""",
            unsafe_allow_html=True,
        )
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            display_mv = "******" if hide_amounts else f"¥{total_mv:,.0f}"
            st.metric(
                label="总市值",
                value=display_mv,
            )
        with col2:
            sharpe_1y = portfolio_1y.get("portfolio_sharpe", 0)
            sharpe_1m = portfolio_1m.get("portfolio_sharpe", 0)
            st.metric(
                label="组合夏普 (1Y)",
                value=f"{sharpe_1y:.2f}",
                delta=f"{sharpe_1m:.2f} (近1月)",
            )
        with col3:
            ret_1y = portfolio_1y.get("weighted_return", 0)
            ret_1m = portfolio_1m.get("weighted_return", 0)
            st.metric(
                label="近1年收益",
                value=f"{ret_1y:.2%}",
                delta=f"{ret_1m:.2%} (近1月)",
            )
        with col4:
            vol_1y = portfolio_1y.get("weighted_volatility", 0)
            st.metric(
                label="加权波动率 (1Y)",
                value=f"{vol_1y:.2%}",
            )
    
    st.divider()
    
    # ===== 区块 2：资产大类配比 + 平台分布 =====
    st.markdown("### 资产配置")
    st.caption("环形图展示资产大类占比，堆叠条形图展示各平台内部构成")
    
    col_left, col_right = st.columns([1, 1.5])
    
    with col_left:
        # 环形图：equity / bond / commodity / cash
        level2_data = get_level2_aggregated(summaries)
        if level2_data:
            labels = list(level2_data.keys())
            values = list(level2_data.values())
            fig_donut = create_donut_chart(labels, values, "资产大类配比",
                                           hide_values=hide_amounts)
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("暂无资产大类数据")
    
    with col_right:
        # 堆叠条形图：每个平台一条
        platform_data = get_platform_breakdown(summaries)
        if platform_data:
            fig_bar = create_horizontal_stacked_bar(platform_data, "各平台资产分布",
                                                    hide_values=hide_amounts)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("暂无平台分布数据")
    
    st.divider()
    
    # ===== 区块 3：币种敞口（环形图） =====
    st.markdown("### 币种敞口")
    st.caption("展示各币种等值 CNY 占比")
    
    currency_data = get_currency_aggregated(summaries)
    if currency_data:
        fig_currency = create_currency_donut(currency_data, "币种敞口",
                                             hide_values=hide_amounts)
        st.plotly_chart(fig_currency, use_container_width=True)
    else:
        st.info("暂无币种数据")
