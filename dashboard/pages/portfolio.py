"""
Tab 3：组合分析 —— "整体表现 + 分散化程度"
组合表现 KPI + 资产大类相关性热力图 + 风险收益散点图
"""

import streamlit as st
import pandas as pd

from dashboard.utils.data_loader import (
    load_indicator_results,
    load_classified_holdings,
    get_portfolio_metrics,
    get_correlation_matrix,
    get_single_asset_metrics,
)
from dashboard.utils.chart_helper import (
    create_heatmap,
    create_scatter_risk_return,
)


def _evaluate_correlation(value: float) -> str:
    """评价相关性等级"""
    abs_val = abs(value)
    if abs_val < 0.2:
        return "低 ✅"
    elif abs_val < 0.5:
        return "中 ⚠️"
    else:
        return "高 ❌"


def render():
    """渲染组合分析页面"""
    indicator_data = load_indicator_results()
    
    # ===== 区块 1：组合表现 KPI =====
    st.markdown("### 组合表现")
    
    # 内嵌控件：时间窗口切换
    window = st.radio(
        "时间窗口",
        ["1M", "1Y"],
        horizontal=True,
        index=1,
        key="portfolio_window",
    )
    window_label = "近1月" if window == "1M" else "近1年"
    
    portfolio = get_portfolio_metrics(indicator_data, window)
    
    if portfolio:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                label=f"加权收益 ({window_label})",
                value=f"{portfolio.get('weighted_return', 0):.2%}",
            )
        with col2:
            st.metric(
                label=f"加权波动率 ({window_label})",
                value=f"{portfolio.get('weighted_volatility', 0):.2%}",
            )
        with col3:
            st.metric(
                label=f"组合夏普 ({window_label})",
                value=f"{portfolio.get('portfolio_sharpe', 0):.4f}",
            )
    else:
        st.warning(f"未找到 {window} 窗口的组合指标数据")
    
    st.divider()
    
    # ===== 区块 2：资产大类相关性 =====
    st.markdown("### 资产大类相关性")
    st.caption("相关性越低，分散化越好；负相关资产能对冲风险")
    
    corr_data = get_correlation_matrix(indicator_data, window)
    
    if corr_data and corr_data.get("labels") and corr_data.get("matrix"):
        labels = corr_data["labels"]
        matrix = corr_data["matrix"]
        
        col_left, col_right = st.columns([1, 1])
        
        with col_left:
            fig_heatmap = create_heatmap(labels, matrix, "相关性矩阵")
            st.plotly_chart(fig_heatmap, use_container_width=True)
        
        with col_right:
            st.markdown("**当前评价：**")
            # 两两对比评价
            for i in range(len(labels)):
                for j in range(i + 1, len(labels)):
                    corr_val = matrix[i][j]
                    eval_text = _evaluate_correlation(corr_val)
                    st.markdown(f"- **{labels[i]} ↔ {labels[j]}**: {corr_val:.4f} ({eval_text})")
            
            # 总体评价
            off_diagonal = []
            for i in range(len(matrix)):
                for j in range(i + 1, len(matrix)):
                    off_diagonal.append(abs(matrix[i][j]))
            
            if off_diagonal:
                avg_corr = sum(off_diagonal) / len(off_diagonal)
                if avg_corr < 0.3:
                    st.success("🎯 总体分散化程度：良好")
                elif avg_corr < 0.5:
                    st.warning("⚠️ 总体分散化程度：一般")
                else:
                    st.error("❌ 总体分散化程度：较差")
            
            st.caption(f"数据点数：{corr_data.get('data_points', 'N/A')}")
    else:
        st.info(f"当前窗口 ({window}) 无足够数据计算相关性矩阵")
    
    st.divider()
    
    # ===== 区块 3：风险收益全景散点图 =====
    st.markdown("### 风险收益全景")
    st.caption("横轴=波动率（风险），纵轴=年化收益（回报），理想位置=左上角")
    
    # 获取单资产指标
    df_metrics = get_single_asset_metrics(indicator_data)
    df_window = df_metrics[df_metrics["window"] == window].copy()
    
    if df_window.empty:
        st.info("暂无单资产指标数据")
        return
    
    # 合并持仓信息（获取 asset_class 和 market_value_cny）
    df_holdings = load_classified_holdings()
    
    if not df_holdings.empty:
        # 按 code 合并 asset_class 和 market_value_cny
        holdings_info = df_holdings.groupby("code").agg({
            "asset_class": "first",
            "market_value_cny": "sum",
        }).reset_index()
        
        df_scatter = df_window.merge(holdings_info, on="code", how="left")
        # 填充缺失值
        df_scatter["asset_class"] = df_scatter["asset_class"].fillna("other")
        df_scatter["market_value_cny"] = df_scatter["market_value_cny"].fillna(1000)
    else:
        df_scatter = df_window.copy()
        df_scatter["asset_class"] = "other"
        df_scatter["market_value_cny"] = 1000
    
    # 过滤掉异常值（波动率或收益率为 None）
    df_scatter = df_scatter.dropna(subset=["volatility", "annualized_return"])
    
    if not df_scatter.empty:
        fig_scatter = create_scatter_risk_return(df_scatter, "资产的年化收益和波动率散点图")
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("暂无可绘制的散点数据")
