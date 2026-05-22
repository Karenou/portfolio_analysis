"""
Portfolio Dashboard - Streamlit 入口
Tab 路由：概览 | 持仓结构 | 组合分析 | 个股体检
"""

import sys
from pathlib import Path

import streamlit as st

# 确保项目根目录在 sys.path 中，方便 import
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.pages import overview, holdings, portfolio, asset_metrics

# ===== 页面配置 =====
st.set_page_config(
    page_title="Portfolio Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ===== 隐藏侧边栏 + 白色底色 + 全局字体颜色修复 =====
st.markdown(
    """
    <style>
        /* 隐藏侧边栏 */
        [data-testid="stSidebar"] { display: none; }
        [data-testid="stSidebarNav"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        section[data-testid="stSidebar"] { display: none; }

        /* 白色底色主题 */
        .stApp {
            background-color: #FFFFFF;
            color: #333333;
        }
        [data-testid="stHeader"] {
            background-color: #FFFFFF;
        }

        /* 全局文本颜色强制黑色 */
        .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
            color: #333333;
        }
        .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6 {
            color: #222222;
        }

        /* Tab 标签颜色 */
        .stTabs [data-baseweb="tab-list"] button {
            color: #333333;
        }
        .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
            color: #E76F51;
        }
        .stTabs [data-baseweb="tab-list"] button:hover {
            color: #264653;
        }

        /* KPI 指标卡 - 统一为一个大背景块 */
        [data-testid="stMetric"] {
            background-color: transparent;
            padding: 12px;
        }
        [data-testid="stMetricLabel"] {
            color: #666666 !important;
        }
        [data-testid="stMetricLabel"] label,
        [data-testid="stMetricLabel"] p,
        [data-testid="stMetricLabel"] div {
            color: #666666 !important;
        }
        [data-testid="stMetricValue"] {
            color: #222222 !important;
        }
        [data-testid="stMetricValue"] div {
            color: #222222 !important;
        }
        [data-testid="stMetricDelta"] {
            color: #E76F51 !important;
        }

        /* Toggle 开关文字颜色 + 边框 */
        .stCheckbox label, .stToggle label,
        [data-testid="stWidgetLabel"] label,
        [data-testid="stWidgetLabel"] p {
            color: #333333 !important;
        }
        /* Toggle 滑块加边框使其在白色背景下可见 */
        [data-testid="stToggle"] > label > div[role="checkbox"] {
            border: 1.5px solid #CCCCCC !important;
        }
        /* Toggle 未选中时：灰色背景 + 左划效果 */
        [data-testid="stToggle"] > label > div[role="checkbox"][aria-checked="false"] {
            background-color: #CCCCCC !important;
            border-color: #BBBBBB !important;
        }
        [data-testid="stToggle"] > label > div[role="checkbox"][aria-checked="false"] > div {
            background-color: #FFFFFF !important;
        }
        /* Toggle 选中时：主题色 */
        [data-testid="stToggle"] > label > div[role="checkbox"][aria-checked="true"] {
            background-color: #E76F51 !important;
            border-color: #E76F51 !important;
        }

        /* Caption / 小字说明 */
        .stCaption, [data-testid="stCaptionContainer"] {
            color: #888888 !important;
        }

        /* Selectbox 选中项 + 下拉菜单选项：统一白色文字 */
        [data-baseweb="select"] *,
        [data-baseweb="popover"] *,
        [data-baseweb="menu"] *,
        [data-baseweb="menu"] li,
        [data-baseweb="menu"] [role="option"],
        ul[role="listbox"] li,
        ul[role="listbox"] li * {
            color: #FFFFFF !important;
        }
        [data-baseweb="select"] svg {
            fill: #FFFFFF !important;
        }

        /* Divider 分割线 */
        [data-testid="stDivider"], hr {
            border-color: #E0E0E0;
        }

        /* Plotly 图表标题和图例在白色背景下的适配 */
        .js-plotly-plot .plotly .gtitle {
            fill: #333333 !important;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ===== 标题 =====
st.markdown(
    """
    <h1 style='text-align: center; margin-bottom: 0; color: #333;'>持仓分析</h1>
    """,
    unsafe_allow_html=True,
)

# ===== Tab 路由 =====
tab1, tab2, tab3, tab4 = st.tabs(["📋 概览", "🏗️ 持仓结构", "📈 组合分析", "🔬 个股体检"])

with tab1:
    overview.render()

with tab2:
    holdings.render()

with tab3:
    portfolio.render()

with tab4:
    asset_metrics.render()

# ===== 底部信息 =====
st.divider()
st.caption("数据来源：本地缓存 | 最近更新时间：参见 indicator_results.json 的 computed_at 字段")
