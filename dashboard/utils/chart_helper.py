"""
图表生成辅助函数
统一管理 Plotly 图表的主题、色板和通用配置
"""

import plotly.graph_objects as go
import plotly.express as px

# 统一色板（按资产大类配色）
ASSET_COLORS = {
    "equity": "#264653",      # 深海蓝绿 - 权益类
    "bond": "#2A9D8F",        # 松石绿 - 固收类
    "commodity": "#E9C46A",   # 沙黄色 - 另类资产
    "cash": "#F4A261",        # 柔和浅橙 - 现金/货币
    "other": "#E76F51",       # 赭红色 - 其他
}

PLATFORM_COLORS = {
    "futu": "#FF6B35",
    "huatai": "#4ECDC4",
    "alipay": "#1A73E8",
    "qieman": "#FFD93D",
    "snowball": "#6C5CE7",
}

# 币种配色（与资产色系统一）
CURRENCY_COLORS = {
    "CNY": "#264653",         # 深海蓝绿
    "HKD": "#2A9D8F",         # 松石绿
    "USD": "#E9C46A",         # 沙黄色
}

# 中英文翻译映射
LABEL_CN = {
    # 资产大类
    "equity": "股票",
    "bond": "债券",
    "commodity": "商品",
    "cash": "现金",
    "other": "其他",
    # 平台
    "futu": "富途",
    "huatai": "华泰",
    "alipay": "支付宝",
    "qieman": "且慢",
    "snowball": "雪球",
    # 币种
    "CNY": "人民币",
    "HKD": "港币",
    "USD": "美元",
}


def translate_label(label: str) -> str:
    """将英文标签翻译为中文"""
    return LABEL_CN.get(label, label)


# 通用布局配置（浅色主题）
LIGHT_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="rgba(255,255,255,1)",
    plot_bgcolor="rgba(255,255,255,1)",
    font=dict(family="PingFang SC, Microsoft YaHei, sans-serif", size=12, color="#333333"),
    margin=dict(l=40, r=40, t=40, b=40),
    title_font=dict(color="#333333"),
    legend_font=dict(color="#333333"),
)

# 向后兼容别名
DARK_LAYOUT = LIGHT_LAYOUT


def apply_theme(fig: go.Figure) -> go.Figure:
    """给图表应用浅色主题"""
    fig.update_layout(**LIGHT_LAYOUT)
    return fig


def create_donut_chart(labels: list, values: list, title: str = "",
                       hide_values: bool = False) -> go.Figure:
    """创建环形图"""
    cn_labels = [translate_label(l) for l in labels]
    colors = [ASSET_COLORS.get(label, "#888888") for label in labels]
    
    fig = go.Figure(data=[go.Pie(
        labels=cn_labels,
        values=values,
        hole=0.5,
        marker=dict(colors=colors),
        textinfo="label+percent",
        textposition="outside",
        texttemplate="%{label}<br>%{percent:.1%}",
        hovertemplate="%{label}<br>¥%{value:,.0f}<br>%{percent:.1%}<extra></extra>",
    )])
    fig.update_layout(
        **LIGHT_LAYOUT,
        title=title,
        showlegend=False,
        height=350,
    )
    return fig


def create_horizontal_stacked_bar(data: dict, title: str = "",
                                  hide_values: bool = False) -> go.Figure:
    """
    创建水平堆叠条形图
    data: {platform_name: {category: value, ...}, ...}
    """
    fig = go.Figure()
    
    # 收集所有类别
    all_categories = set()
    for platform_data in data.values():
        all_categories.update(platform_data.keys())
    all_categories = sorted(all_categories)
    
    platforms = list(data.keys())
    cn_platforms = [translate_label(p) for p in platforms]
    
    for category in all_categories:
        values = [data[p].get(category, 0) for p in platforms]
        fig.add_trace(go.Bar(
            name=translate_label(category),
            y=cn_platforms,
            x=values,
            orientation="h",
            marker_color=ASSET_COLORS.get(category, "#888888"),
            hovertemplate="%{y} - " + translate_label(category) + "<br>¥%{x:,.0f}<extra></extra>",
        ))
    
    fig.update_layout(
        **LIGHT_LAYOUT,
        title=title,
        barmode="stack",
        height=max(200, len(platforms) * 60 + 80),
        xaxis_title="" if hide_values else "市值 (CNY)",
        xaxis=dict(
            tickformat=",",
            tickcolor="#333333",
            tickfont=dict(color="#333333"),
        ),
        yaxis=dict(
            tickfont=dict(color="#333333"),
        ),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    
    # 隐藏模式下不显示 x 轴刻度
    if hide_values:
        fig.update_xaxes(showticklabels=False)
    
    return fig


def create_currency_donut(currency_data: dict, title: str = "",
                          hide_values: bool = False) -> go.Figure:
    """创建币种敞口环形图"""
    labels = list(currency_data.keys())
    values = list(currency_data.values())
    cn_labels = [translate_label(l) for l in labels]
    colors = [CURRENCY_COLORS.get(label, "#888888") for label in labels]
    
    fig = go.Figure(data=[go.Pie(
        labels=cn_labels,
        values=values,
        hole=0.5,
        marker=dict(colors=colors),
        textinfo="label+percent",
        textposition="outside",
        texttemplate="%{label}<br>%{percent:.1%}",
        hovertemplate="%{label}<br>¥%{value:,.0f}<br>%{percent:.1%}<extra></extra>",
    )])
    fig.update_layout(
        **LIGHT_LAYOUT,
        title=title,
        showlegend=False,
        height=350,
    )
    return fig


def create_currency_bar(currency_data: dict, title: str = "",
                        hide_values: bool = False) -> go.Figure:
    """创建币种敞口水平条形图（保留向后兼容）"""
    return create_currency_donut(currency_data, title, hide_values)


def create_treemap(df, title: str = "") -> go.Figure:
    """创建矩形树图（Treemap）"""
    fig = px.treemap(
        df,
        path=["asset_class", "name"],
        values="market_value_cny",
        color="asset_class",
        color_discrete_map=ASSET_COLORS,
        title=title,
    )
    fig.update_layout(
        **LIGHT_LAYOUT,
        height=500,
    )
    return fig


def create_heatmap(labels: list, matrix: list, title: str = "") -> go.Figure:
    """创建相关性热力图"""
    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=labels,
        y=labels,
        colorscale="RdBu_r",
        zmin=-1,
        zmax=1,
        text=[[f"{v:.3f}" for v in row] for row in matrix],
        texttemplate="%{text}",
        textfont={"size": 14},
    ))
    fig.update_layout(
        **LIGHT_LAYOUT,
        title=title,
        height=350,
        width=400,
        xaxis=dict(tickfont=dict(color="#333333")),
        yaxis=dict(tickfont=dict(color="#333333")),
    )
    return fig


def create_scatter_risk_return(df, title: str = "") -> go.Figure:
    """创建风险收益散点图"""
    fig = px.scatter(
        df,
        x="volatility",
        y="annualized_return",
        size="market_value_cny",
        color="asset_class",
        color_discrete_map=ASSET_COLORS,
        hover_name="name",
        title=title,
        labels={
            "volatility": "波动率（风险）",
            "annualized_return": "年化收益（回报）",
        },
    )
    # 添加无风险利率水平线
    fig.add_hline(
        y=0.018,
        line_dash="dash",
        line_color="#999999",
        opacity=0.5,
        annotation_text="无风险利率 1.8%",
    )
    fig.update_layout(
        **LIGHT_LAYOUT,
        height=500,
        xaxis=dict(
            title=dict(text="波动率（风险）", font=dict(color="#333333")),
            tickfont=dict(color="#333333"),
        ),
        yaxis=dict(
            title=dict(text="年化收益（回报）", font=dict(color="#333333")),
            tickfont=dict(color="#333333"),
        ),
    )
    return fig


def create_nav_line(df, title: str = "", value_col: str = "normalized_nav", y_label: str = "归一化净值") -> go.Figure:
    """创建净值/价格折线图"""
    # 自动识别日期列
    date_col = None
    for col_name in ["trade_date", "nav_date", "date"]:
        if col_name in df.columns:
            date_col = col_name
            break
    if date_col is None:
        date_col = df.columns[0]  # fallback: 第一列
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[value_col],
        mode="lines",
        line=dict(color="#264653", width=2),
    ))
    fig.update_layout(
        **LIGHT_LAYOUT,
        title=title,
        height=350,
        xaxis=dict(
            title=dict(text="日期", font=dict(color="#333333")),
            tickfont=dict(color="#333333"),
            gridcolor="#E0E0E0",
            showgrid=True,
        ),
        yaxis=dict(
            title=dict(text=y_label, font=dict(color="#333333")),
            tickfont=dict(color="#333333"),
            autorange=True,
            gridcolor="#E0E0E0",
            showgrid=True,
        ),
    )
    return fig
