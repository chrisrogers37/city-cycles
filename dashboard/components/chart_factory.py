"""
Chart factory -- creates consistently-themed Plotly figures.
All charts use the atmospheric template and consistent color palettes.
"""

import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def monthly_trend_chart(df: pd.DataFrame, y_col: str, y_label: str,
                        title: str, color_col: str = 'year') -> go.Figure:
    """Create a monthly trend line chart with year overlay."""
    fig = px.line(df, x='month', y=y_col, color=color_col,
                  title=title, labels={y_col: y_label, 'month': 'Month'},
                  template='atmospheric')
    fig.update_xaxes(
        tickmode='array',
        tickvals=list(range(1, 13)),
        ticktext=MONTH_LABELS,
    )
    return fig


def hourly_bar_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create an hourly distribution bar chart."""
    fig = px.bar(df, x='hour_of_day', y='ride_count', title=title,
                 template='atmospheric',
                 color_discrete_sequence=['#5DADE2'])
    fig.update_layout(bargap=0.15)
    return fig


def comparison_line_chart(df: pd.DataFrame, x_col: str, y_col: str,
                          title: str, y_label: str, x_label: str) -> go.Figure:
    """Create a comparison line chart colored by location."""
    fig = px.line(df, x=x_col, y=y_col, color='location',
                  title=title,
                  labels={y_col: y_label, x_col: x_label},
                  template='atmospheric')
    return fig


def grouped_bar_chart(df: pd.DataFrame, x_col: str, y_col: str,
                      title: str, y_label: str) -> go.Figure:
    """Create a grouped bar chart colored by location."""
    fig = px.bar(df, x=x_col, y=y_col, color='location',
                 barmode='group', title=title,
                 labels={y_col: y_label},
                 template='atmospheric')
    return fig


def station_growth_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create a station count bar chart."""
    fig = px.bar(df, x='year', y='metric_value', title=title,
                 template='atmospheric',
                 color_discrete_sequence=['#5DADE2'])
    return fig
