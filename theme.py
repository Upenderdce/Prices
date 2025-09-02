# =====================
# THEME TOGGLE
# =====================

def apply_theme(light_mode: bool):
    if light_mode:
        custom_css = """
        <style>
        .stApp { background-color: #FFFFFF; color: #000000; }
        section[data-testid="stSidebar"] { background-color: #F5F5F5; color: #000000; }
        .stMultiSelect div[data-baseweb="tag"] { background-color: #e0e0e0; color: #000000; }
        .stButton>button { background-color: #007bff; color: #FFFFFF; border-radius: 8px; }
        .js-plotly-plot .plotly { background-color: #FFFFFF !important; }
        table { border: 1px solid #e0e0e0; }
        /* ✅ Fix all widget labels/text for light mode */
        div[data-testid="stMarkdown"], div[data-testid="stWidgetLabel"], 
        label, span, p, h1, h2, h3, h4, h5, h6 {
            color: #000000 !important;
        }
        /* Tabs styling */
        button[data-baseweb="tab"] {
            color: #000000 !important;
            border-bottom: none !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            color: #007bff !important;
            font-weight: bold;
            border-bottom: 2px solid #007bff !important;
        }
        </style>
        """
        return custom_css, "#FFFFFF", "black"   # css, plot_bg, font
    else:
        custom_css = """
            <style>
            .stApp { background-color: #181818; color: #FFFFFF; }
            section[data-testid="stSidebar"] { background-color: #262626; color: #FFFFFF; }
            .stMultiSelect div[data-baseweb="tag"] { background-color: #444444; color: #FFFFFF; }
            .stButton>button { background-color: #007bff; color: #FFFFFF; border-radius: 8px; }
            .js-plotly-plot .plotly { background-color: #181818 !important; }
            table { border: 1px solid #444444; }
            /* ✅ Fix all widget labels/text for dark mode */
            div[data-testid="stMarkdown"], div[data-testid="stWidgetLabel"], 
            label, span, p, h1, h2, h3, h4, h5, h6 {
                color: #FFFFFF !important;
            }
            /* Tabs styling */
            button[data-baseweb="tab"] {
                color: #FFFFFF !important;
                border-bottom: none !important;
            }
            button[data-baseweb="tab"][aria-selected="true"] {
                color: #66b3ff !important;
                font-weight: bold;
                border-bottom: 2px solid #66b3ff !important;
            }
            </style>
            """
        return custom_css, "#181818", "white"   # css, plot_bg, font
