"""
Tab 4：个股体检 —— "逐一审视每个资产"
资产选择器 + KPI + 净值走势 + 排名 + 全资产对比表
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

from dashboard.utils.data_loader import (
    load_indicator_results,
    load_classified_holdings,
    get_single_asset_metrics,
    load_nav_history,
    get_db_tables,
)
from dashboard.utils.chart_helper import create_nav_line, DARK_LAYOUT


def render():
    """渲染个股体检页面"""
    indicator_data = load_indicator_results()
    df_metrics = get_single_asset_metrics(indicator_data)
    df_holdings = load_classified_holdings()
    
    if df_metrics.empty:
        st.warning("未找到资产指标数据")
        return
    
    # ===== 区块 1：资产选择器 =====
    # 构建选项列表：code - name
    unique_assets = df_metrics[["code", "name"]].drop_duplicates()
    asset_options = {f"{row['name']} ({row['code']})": row["code"] 
                     for _, row in unique_assets.iterrows()}
    
    col1, col2 = st.columns(2)
    with col1:
        selected_label = st.selectbox(
            "选择资产",
            list(asset_options.keys()),
            key="asset_selector",
        )
    with col2:
        window = st.radio(
            "时间窗口",
            ["1M", "3M", "6M", "1Y", "5Y", "10Y"],
            horizontal=True,
            index=3,
            key="asset_window",
        )
    
    selected_code = asset_options[selected_label]
    window_label_map = {
        "1M": "近1月", "3M": "近3月", "6M": "近6月",
        "1Y": "近1年", "5Y": "近5年", "10Y": "近10年",
    }
    window_label = window_label_map.get(window, window)
    
    st.divider()
    
    # ===== 区块 2：选中资产 KPI =====
    asset_data = df_metrics[
        (df_metrics["code"] == selected_code) & (df_metrics["window"] == window)
    ]
    
    # 获取资产名称（从任意 window 取）
    asset_any = df_metrics[df_metrics["code"] == selected_code]
    asset_name = asset_any.iloc[0]["name"] if not asset_any.empty else selected_label
    
    st.markdown(f"### {asset_name} 体检报告 ({window_label})")
    
    if asset_data.empty:
        st.caption(f"⚠️ 暂无 {window} 窗口的计算指标（仅支持 1M/1Y）")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("年化收益", "N/A")
        with col2:
            st.metric("波动率", "N/A")
        with col3:
            st.metric("最大回撤", "N/A")
        with col4:
            st.metric("夏普比率", "N/A")
    else:
        row = asset_data.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            ann_ret = row["annualized_return"]
            st.metric("年化收益", f"{ann_ret:.2%}" if pd.notna(ann_ret) else "N/A")
        with col2:
            vol = row["volatility"]
            st.metric("波动率", f"{vol:.2%}" if pd.notna(vol) else "N/A")
        with col3:
            mdd = row["max_drawdown"]
            st.metric("最大回撤", f"{mdd:.2%}" if pd.notna(mdd) else "N/A")
        with col4:
            sharpe = row["sharpe"]
            st.metric("夏普比率", f"{sharpe:.4f}" if pd.notna(sharpe) else "N/A")
    
    st.divider()
    
    # ===== 区块 3：价格走势 =====
    st.markdown("### 价格走势")
    
    # 根据时间窗口计算截取的起始日期
    window_delta_map = {
        "1M": relativedelta(months=1),
        "3M": relativedelta(months=3),
        "6M": relativedelta(months=6),
        "1Y": relativedelta(years=1),
        "5Y": relativedelta(years=5),
        "10Y": relativedelta(years=10),
    }
    
    nav_df = load_nav_history(selected_code)
    if nav_df is not None and not nav_df.empty:
        # 直接展示原始价格/净值，不做归一化
        if "nav" in nav_df.columns:
            nav_col = "nav"
        elif "close" in nav_df.columns:
            nav_col = "close"
        elif "adj_close" in nav_df.columns:
            nav_col = "adj_close"
        else:
            # 找第一个数值列
            numeric_cols = nav_df.select_dtypes(include="number").columns.tolist()
            nav_col = numeric_cols[0] if numeric_cols else None
        
        if nav_col:
            # 识别日期列并按时间窗口截取
            date_col = None
            for col_name in ["trade_date", "nav_date", "date"]:
                if col_name in nav_df.columns:
                    date_col = col_name
                    break
            if date_col is None:
                date_col = nav_df.columns[0]
            
            nav_df[date_col] = pd.to_datetime(nav_df[date_col])
            cutoff_date = datetime.now() - window_delta_map[window]
            nav_df_filtered = nav_df[nav_df[date_col] >= cutoff_date].copy()
            
            if nav_df_filtered.empty:
                nav_df_filtered = nav_df.copy()
            
            nav_df_filtered["plot_value"] = nav_df_filtered[nav_col]
            fig_nav = create_nav_line(nav_df_filtered, f"{asset_name}", value_col="plot_value", y_label="价格/净值")
            st.plotly_chart(fig_nav, use_container_width=True)
        else:
            st.info("净值数据列格式不匹配")
    else:
        st.info(f"暂无 {asset_name} 的净值历史数据")
        # 显示数据库表信息（调试用）
        tables = get_db_tables()
        if tables:
            st.caption(f"数据库可用表：{', '.join(tables)}")
    
    st.divider()
    
    # ===== 区块 4：排名信息 =====
    st.markdown("### 排名信息")
    
    # 排名使用最接近的可用窗口（fallback 到 1Y）
    rank_window = window if window in ["1M", "1Y"] else "1Y"
    df_window = df_metrics[df_metrics["window"] == rank_window].copy()
    total_assets = len(df_window)
    
    if df_window.empty:
        st.info("暂无排名数据")
    else:
        # 夏普排名（越高越好）
        df_window["sharpe_rank"] = df_window["sharpe"].rank(ascending=False, method="min")
        # 回撤排名（绝对值越小越好，即越接近0越好）
        df_window["mdd_rank"] = df_window["max_drawdown"].abs().rank(ascending=True, method="min")
        
        selected_row = df_window[df_window["code"] == selected_code]
        if not selected_row.empty:
            sharpe_rank = int(selected_row.iloc[0]["sharpe_rank"])
            mdd_rank = int(selected_row.iloc[0]["mdd_rank"])
            st.markdown(
                f"在 **{total_assets}** 个持仓中（{rank_window}窗口），"
                f"**夏普比率**排第 **{sharpe_rank}** 名，"
                f"**回撤控制**排第 **{mdd_rank}** 名"
            )
    
    st.divider()
    
    # ===== 区块 5：全资产对比表 =====
    st.markdown("### 全资产对比表")
    
    # 内嵌控件
    col1, col2 = st.columns(2)
    with col1:
        sort_metric = st.selectbox(
            "排序指标",
            ["sharpe", "annualized_return", "volatility", "max_drawdown"],
            format_func=lambda x: {
                "sharpe": "夏普比率",
                "annualized_return": "年化收益",
                "volatility": "波动率",
                "max_drawdown": "最大回撤",
            }[x],
            key="asset_sort_metric",
        )
    with col2:
        # 类别筛选
        if not df_holdings.empty:
            all_classes = ["全部"] + sorted(df_holdings["asset_class"].unique().tolist())
        else:
            all_classes = ["全部"]
        filter_class = st.selectbox("筛选类别", all_classes, key="asset_filter_class")
    
    # 构建对比表
    df_table = df_window.copy()
    
    # 合并 asset_class
    if not df_holdings.empty:
        holdings_class = df_holdings.groupby("code")["asset_class"].first().reset_index()
        df_table = df_table.merge(holdings_class, on="code", how="left")
        df_table["asset_class"] = df_table["asset_class"].fillna("other")
    else:
        df_table["asset_class"] = "other"
    
    # 筛选
    if filter_class != "全部":
        df_table = df_table[df_table["asset_class"] == filter_class]
    
    # 排序
    ascending = True if sort_metric in ["volatility", "max_drawdown"] else False
    df_table = df_table.sort_values(sort_metric, ascending=ascending, na_position="last")
    df_table["排名"] = range(1, len(df_table) + 1)
    
    # 展示列
    display_df = df_table[["排名", "name", "annualized_return", "volatility", 
                           "max_drawdown", "sharpe", "asset_class"]].copy()
    display_df.columns = ["排名", "名称", "年化收益", "波动率", "最大回撤", "夏普", "类别"]
    
    # 格式化百分比
    for col in ["年化收益", "波动率", "最大回撤"]:
        display_df[col] = display_df[col].apply(
            lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
        )
    display_df["夏普"] = display_df["夏普"].apply(
        lambda x: f"{x:.4f}" if pd.notna(x) else "N/A"
    )
    
    # 高亮当前选中资产
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=500,
    )
