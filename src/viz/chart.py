import pandas as pd
import streamlit as st

def LineChart(data, x: str, y: str):
    st.line_chart(data, width=0, height=0, use_container_width=True)
