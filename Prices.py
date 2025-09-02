import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
from datetime import datetime, date
import textwrap
import streamlit_sortables as sortables
import initialization
import scraping
import theme

st.set_page_config(page_title="Car Price Dashboard", layout="wide")

# =====================
# SIDEBAR
# =====================
with st.sidebar:
    st.header("⚙️ Settings")
    light_mode = st.toggle("🌞 Light Mode", value=False)
    custom_css, plot_bgcolor, font_color = theme.apply_theme(light_mode)
    st.markdown(custom_css, unsafe_allow_html=True)

    if st.button("🔄 Fetch Latest Prices"):
        with st.spinner("Calling brand APIs in parallel..."):
            scraped = scraping.scrape_all_brands_parallel()
            if scraped:
                initialization.store_prices(scraped)
                st.success(f"Scraped & stored {len(scraped)} records.")
            else:
                st.error("No prices scraped.")

# =====================
# MAIN TITLE
# =====================
st.title("🚗 Car Price Dashboard")
initialization.init_db()
df = initialization.get_latest_prices()
if df.empty:
    st.info("No data yet. Use **Fetch Latest Prices** from the sidebar.")
    st.stop()

# Ensure numeric
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["price_lakhs"] = (df["price"] / 100000).round(2)


st.sidebar.header("Filters")
brands_available = sorted(df["brand"].unique())
selected_brands = st.sidebar.multiselect("Brand(s)", options=brands_available, default=[])

models_available = sorted(df[df["brand"].isin(selected_brands)]["model"].unique())
selected_models = st.sidebar.multiselect("Model(s)", options=models_available, default=models_available)

fuel_available = sorted(df[df["brand"].isin(selected_brands) & df["model"].isin(selected_models)]["fuel"].unique())
selected_fuel = st.sidebar.multiselect("Fuel(s)", options=fuel_available, default=fuel_available)

trans_available = sorted(df[df["brand"].isin(selected_brands) & df["model"].isin(selected_models)]["transmission"].unique())
selected_trans = st.sidebar.multiselect("Transmission(s)", options=trans_available, default=trans_available)

min_price = int(df["price_lakhs"].min())
max_price = int(df["price_lakhs"].max())
price_range = st.sidebar.slider(
    "Price Range (₹ Lakhs)",
    min_value=min_price,
    max_value=max_price,
    value=(min_price, max_price)
)
# Apply all filters
df_filtered = df[
    df["brand"].isin(selected_brands) &
    df["model"].isin(selected_models) &
    df["fuel"].isin(selected_fuel) &
    df["transmission"].isin(selected_trans) &
    (df["price_lakhs"] >= price_range[0]) &
    (df["price_lakhs"] <= price_range[1])
    ].copy()

if df_filtered.empty:
    st.warning("No data matches selected filters.")
    st.stop()


# =====================
# TABS
# =====================
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Dashboard", "📋 Price Table", "📜 Price History", "🛠 Manage Entries"]
)

with tab1:
    st.subheader("Visual Analytics")


    df_filtered["variant_display"] = df_filtered.apply(
        lambda r: f"{r['model']} - {r['variant']}",
        axis=1)
    print(df_filtered)

    variant_map = dict(zip(df_filtered["variant_display"], df_filtered["variant"]))

    # All options
    all_variants_display = df_filtered["variant_display"].unique().tolist()

    # Multiselect with Model + Variant
    selected_variants_display = st.multiselect(
        "Select Variants to Show",
        options=all_variants_display,
        default=all_variants_display
    )

    # Map back to actual variants
    selected_variants = [variant_map[v] for v in selected_variants_display]

    # ✅ Filter the dataframe itself
    df_filtered = df_filtered[df_filtered["variant"].isin(selected_variants)]

    # Chart labels: only variant name (+ CNG + price)
    df_filtered["label"] = df_filtered.apply(
        lambda r: f"{r['variant']} ({r['price_lakhs']:.2f}L)",
        axis=1
    )

    # ---- Default order (by min price) ----
    model_order = (
        df_filtered.groupby("model", observed=True)["price_lakhs"]
        .min()
        .sort_values()
        .index
        .tolist()
    )

    st.sidebar.subheader("📋 Arrange Models")
    st.markdown("""
        <style>
        .sortable-item {
            padding: 4px 8px !important;
            margin: 2px 0 !important;
            font-size: 0.85rem !important;
        }
        .sortable-container {
            padding: 2px !important;
        }
        </style>
    """, unsafe_allow_html=True)
    # Drag-and-drop list

    with st.sidebar.expander("📋 Arrange Models", expanded=True):
        current_models = model_order  # always from latest filter

        # Ensure session_state has the right models
        if "final_order" not in st.session_state:
            st.session_state.final_order = current_models
        else:
            # Add missing models
            for m in current_models:
                if m not in st.session_state.final_order:
                    st.session_state.final_order.append(m)
            # Remove ones not in current selection
            st.session_state.final_order = [
                m for m in st.session_state.final_order if m in current_models
            ]

        # --- Pass current models in the latest order ---
        ordered_models = [m for m in st.session_state.final_order if m in current_models]
        custom_order = sortables.sort_items(
            items=ordered_models,
            key=f"sortable_models_{len(ordered_models)}"
        )

        # Handle None or []
        if not custom_order:
            custom_order = ordered_models

        # Update if changed
        if custom_order != st.session_state.final_order:
            st.session_state.final_order = custom_order

        order_to_use = st.session_state.final_order

    st.sidebar.write("👉 Final Order:", order_to_use)

    # ---- Filter data only to selected models ----
    df_filtered["model"] = pd.Categorical(
        df_filtered["model"], categories=order_to_use, ordered=True
    )
    df_filtered = df_filtered.sort_values(["model", "timestamp"])

    # =====================
    # Chart selection
    # =====================
    chart_type = st.radio(
        "Select Chart Type",
        ["Price Range", "Scatter Plot", "Violin Plot", "Line Chart", "Treemap"],  # NEW OPTION
        horizontal=True
    )
    # ---------------------
    # Price Range Chart
    # ---------------------
    if chart_type == "Price Range":
        price_range_df = (
            df_filtered.groupby("model", observed=True)
            .agg(min_price_lakh=("price_lakhs", "min"),
                 max_price_lakh=("price_lakhs", "max"))
            .reset_index()
        )

        fig = go.Figure()

        # Apply custom order
        price_range_df["model"] = pd.Categorical(price_range_df["model"], categories=order_to_use, ordered=True)
        price_range_df = price_range_df.sort_values("model")

        df_filtered["model"] = pd.Categorical(df_filtered["model"], categories=order_to_use, ordered=True)
        df_filtered = df_filtered.sort_values("model")

        fig.add_trace(go.Bar(
            x=price_range_df["model"],
            y=price_range_df["max_price_lakh"] - price_range_df["min_price_lakh"],
            base=price_range_df["min_price_lakh"],
            name="Price Range",
            marker=dict(color="lightblue"),
            opacity=0.4,
            hoverinfo="skip",
            width=0.3
        ))

        fig.add_trace(go.Scatter(
            x=df_filtered["model"],
            y=df_filtered["price_lakhs"],
            mode="markers+text",
            name="Variants",
            text=df_filtered["label"],
            textposition="middle right",
            hovertemplate="<b>%{text}</b><br>Model: %{x}<br>Price: ₹%{y} L<extra></extra>",
            marker=dict(color="dark blue", size=9, line=dict(width=1, color="white")),
            cliponaxis=False
        ))

    # ---------------------
    # Scatter Plot
    # ---------------------
    elif chart_type == "Scatter Plot":
        df_filtered["model"] = pd.Categorical(df_filtered["model"], categories=order_to_use, ordered=True)

        fig = px.scatter(
            df_filtered,
            x="model",
            y="price_lakhs",
            color="fuel",
            size="price_lakhs",
            text="label",
            hover_data=["brand", "variant", "transmission"],
            title="Price of Each Variant by Model & Fuel (₹ Lakhs)",
            category_orders={"model": order_to_use},
            height=520
        )
        fig.update_traces(textposition="middle center")

    # ---------------------
    # Violin Plot
    # ---------------------
    elif chart_type == "Violin Plot":
        df_filtered["model"] = pd.Categorical(df_filtered["model"], categories=order_to_use, ordered=True)

        fig = px.violin(
            df_filtered,
            x="brand",
            y="price_lakhs",
            color="brand",
            box=True,
            points="all",
            title="Price Distribution by Brand (₹ Lakhs)",
            height=520
        )

        scatter = px.scatter(
            df_filtered,
            x="brand",
            y="price_lakhs",
            text="label",
            color="brand"
        )
        scatter.update_traces(textposition="top center", showlegend=False)
        for trace in scatter.data:
            fig.add_trace(trace)

    # ---------------------
    # Line Chart
    # ---------------------
    elif chart_type == "Line Chart":
        df_filtered["model"] = pd.Categorical(df_filtered["model"], categories=order_to_use, ordered=True)
        df_filtered = df_filtered.sort_values("model")

        fig = px.line(
            df_filtered.sort_values("price_lakhs"),
            x="model",
            y="price_lakhs",
            color="brand",
            markers=True,
            text="label",
            title="Price Trends by Model (₹ Lakhs)",
            category_orders={"model": order_to_use},
            height=520
        )
        fig.update_traces(textposition="top center")

    # ---------------------
    # Treemap
    # ---------------------
    elif chart_type == "Treemap":
        df_filtered["model"] = pd.Categorical(df_filtered["model"], categories=order_to_use, ordered=True)
        df_filtered = df_filtered.sort_values(["model", "price_lakhs"])

        df_filtered["variant_treemap_label"] = df_filtered.apply(
            lambda r: "<br>".join(textwrap.wrap(
                f"{r['variant']}", width=12
            )),
            axis=1
        )

        df_sorted = df_filtered.sort_values(["model", "price_lakhs"], ascending=[True, True])

        fig = px.treemap(
            df_sorted,
            path=["brand", "model", "variant_treemap_label"],
            values="price_lakhs",
            color="price_lakhs",
            color_continuous_scale="Blues" if light_mode else "Viridis",
            title="Brand → Model → Variant Price Share",
            hover_data={"brand": True, "model": True, "variant": True, "price_lakhs": ":.2f"}
        )

        fig.update_traces(
            textfont=dict(size=14, family="Arial", color="black" if light_mode else "white"),
            texttemplate="%{label}<br>₹%{value:.2f} L",
            sort=False
        )

    # ---------------------
    # Layout
    # ---------------------
    fig.update_layout(
        title="Model Prices (in Lakhs)",
        xaxis=dict(
            title=dict(text="Model", font=dict(color=font_color)),
            automargin=True,
            rangeslider=dict(visible=False),
            fixedrange=False,
            tickfont=dict(color=font_color),
            gridcolor="lightgrey" if light_mode else "#333333",
            zerolinecolor="lightgrey" if light_mode else "#333333"
        ),
        yaxis=dict(
            title=dict(text="Price (₹ Lakhs)", font=dict(color=font_color)),
            automargin=True,
            fixedrange=False,
            tickfont=dict(color=font_color),
            gridcolor="lightgrey" if light_mode else "#333333",
            zerolinecolor="lightgrey" if light_mode else "#333333"
        ),
        hovermode="closest",
        plot_bgcolor=plot_bgcolor,
        paper_bgcolor=plot_bgcolor,
        font=dict(color=font_color),
        showlegend=False
    )

    st.plotly_chart(fig, use_container_width=True)

# -----------------
# TAB 2: TABLE
# -----------------
with tab2:
    st.subheader("Price Table")
    df_table = df_filtered.sort_values(["brand", "model", "price_lakhs"])[
        ["brand", "model", "fuel", "transmission", "variant", "price_lakhs"]
    ].rename(columns={"price_lakhs": "Price (₹ Lakhs)"})
    df_table["Price (₹ Lakhs)"] = df_table["Price (₹ Lakhs)"].map(lambda x: f"{x:.2f}")
    st.dataframe(df_table, use_container_width=True)


with tab3:
    st.subheader("📈 Price History Viewer")

    def load_price_history(brands, models):
        conn = sqlite3.connect(initialization.DB_FILE)
        # Query only for selected brands and models to reduce data
        query = """
            SELECT * FROM prices
            WHERE brand IN ({})
                AND model IN ({})
            ORDER BY timestamp
        """.format(
            ",".join(["?"] * len(brands)) if brands else "'*'",
            ",".join(["?"] * len(models)) if models else "'*'"
        )
        params = brands + models if brands and models else []
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df

    # Use main app's filtered brands and models
    brands = selected_brands if selected_brands else sorted(df["brand"].unique())
    models = selected_models if selected_models else sorted(df[df["brand"].isin(brands)]["model"].unique())

    # Load historical data for filtered brands and models
    df_history = load_price_history(brands, models)

    if df_history.empty:
        st.warning("No price history found for selected brands and models.")
        st.stop()

    df_history["timestamp"] = pd.to_datetime(df_history["timestamp"], errors="coerce")
    if df_history["timestamp"].isna().any():
        st.warning(f"{df_history['timestamp'].isna().sum()} records dropped due to invalid timestamps.")
        df_history = df_history.dropna(subset=["timestamp"])

    df_history["price_lakhs"] = (df_history["price"] / 100000).round(2)
    df_history["date"] = df_history["timestamp"].dt.date

    if df_history.empty:
        st.warning("No valid data after processing timestamps.")
        st.stop()

    # Allow variant filtering in tab3
    with st.sidebar.expander("Price History Filters", expanded=True):
        variants = st.multiselect(
            "Select Variants",
            sorted(df_history["variant"].unique()),
            default=sorted(df_history["variant"].unique()),
            key="history_variants"
        )

    # Filter by selected variants
    df_filtered_history = df_history[df_history["variant"].isin(variants)].copy()

    if df_filtered_history.empty:
        st.warning("No data for selected variants.")
        st.stop()

    # Aggregate by day, keeping the last price
    df_daywise = (
        df_filtered_history.groupby(["brand", "model", "variant", "date"], observed=True)
        .agg({"price": "last"})
        .reset_index()
    )

    if df_daywise.empty or df_daywise["date"].isna().all():
        st.warning("No valid data available for plotting.")
        st.stop()

    # Fill missing dates using pivot and reindex
    pivot_df = df_daywise.pivot_table(
        index=["brand", "model", "variant"],
        columns="date",
        values="price",
        aggfunc="last"
    )
    all_days = pd.date_range(start=df_daywise["date"].min(), end=date.today(), freq="D")
    pivot_df = pivot_df.reindex(columns=all_days).ffill(axis=1).bfill(axis=1)
    # Rename level_3 to date to fix ValueError
    df_daywise = pivot_df.stack(future_stack=True).reset_index(name="price").rename(columns={"level_3": "date"})
    df_daywise["price_lakhs"] = (df_daywise["price"] / 100000).round(2)
    df_daywise["label"] = df_daywise["brand"] + " | " + df_daywise["model"] + " - " + df_daywise["variant"]

    # Plot line chart
    fig = px.line(
        df_daywise,
        x="date",
        y="price_lakhs",
        color="label",
        title=f"Price Trends ({', '.join(brands)})",
        markers=True,
        line_shape="hv"  # Step line for discrete data points
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Price (₹ Lakhs)",
        plot_bgcolor=plot_bgcolor,
        paper_bgcolor=plot_bgcolor,
        font=dict(color=font_color)
    )
    st.plotly_chart(fig, use_container_width=True)

    # History table (limit to last 7 days for readability)
    st.subheader("📜 Price History Table")
    recent_days = pd.date_range(end=date.today(), periods=7, freq="D")
    df_wide = df_daywise.pivot_table(
        index=["brand", "model", "variant"],
        columns="date",
        values="price_lakhs",
        aggfunc="last"
    ).reset_index()
    df_wide.columns = [str(c) for c in df_wide.columns]
    # Filter to recent days
    display_cols = ["brand", "model", "variant"] + [col for col in df_wide.columns if pd.to_datetime(col, errors="coerce") in recent_days]
    st.dataframe(df_wide[display_cols], use_container_width=True)



# -----------------
# TAB 4: MANAGE
# -----------------
with tab4:
    st.subheader("➕ Add Variant Price")
    # Place the form in main layout (not sidebar)
    with st.form("price_entry_form", clear_on_submit=True):
        cols = st.columns(3)  # 👈 use columns for compact layout

        with cols[0]:
            brand_in = st.text_input("Brand", value="Maruti")
            model_in = st.text_input("Model")
            variant_in = st.text_input("Variant")

        with cols[1]:
            fuel_in = st.text_input("Fuel", value="Petrol")
            trans_in = st.text_input("Transmission", value="Manual")

        with cols[2]:
            # Input price in Lakhs, store in rupees
            price_lakh_in = st.number_input("Price (₹ Lakhs)", min_value=0.0, step=1.0, format="%.2f")

        submitted = st.form_submit_button("Add Price")

        if submitted and brand_in and model_in and variant_in and price_lakh_in > 0:
            price_rupees = int(price_lakh_in * 100000)  # convert lakhs → rupees
            timestamp = datetime.now().isoformat()
            initialization.add_price(brand_in, model_in, variant_in, price_rupees, fuel_in, trans_in, timestamp)
            st.success(
                f"✅ Added {brand_in} {model_in} {variant_in} "
                f"{fuel_in} {trans_in} at ₹{price_rupees:,.0f}"
            )
            st.rerun()

    st.subheader("🗑️ Delete a Manual Entry")

    conn = sqlite3.connect(initialization.DB_FILE)
    df_manual = pd.read_sql(
        "SELECT * FROM prices WHERE source='manual' ORDER BY timestamp DESC", conn
    )
    conn.close()

    if df_manual.empty:
        st.info("No manual entries available to delete.")
    else:
        # Clean label with all details
        df_manual["label"] = (
                df_manual["brand"] + " | " +
                df_manual["model"] + " | " +
                df_manual["variant"] + " | " +
                df_manual["fuel"] + " | " +
                df_manual["transmission"] + " | ₹" +
                df_manual["price"].astype(str)
        )

        # Dropdown with proper labels
        record_choice = st.selectbox(
            "Select entry to delete",
            df_manual[["id", "label"]].itertuples(index=False),
            format_func=lambda x: x.label
        )

        if st.button("Delete Selected Entry"):
            initialization.delete_price(record_choice.id)  # ✅ deletes only manual
            st.success("✅ Manual entry deleted.")
            st.rerun()

import io
import pandas as pd
import xlsxwriter

def to_excel_price_range_chart(df_filtered, order_to_use):
    output = io.BytesIO()

    # --- Prepare dataframes ---
    price_range_df = (
        df_filtered.groupby("model", observed=True)
        .agg(min_price_lakh=("price_lakhs", "min"),
             max_price_lakh=("price_lakhs", "max"))
        .reset_index()
    )

    # enforce order
    price_range_df["model"] = pd.Categorical(
        price_range_df["model"], categories=order_to_use, ordered=True
    )
    price_range_df = price_range_df.sort_values("model").reset_index(drop=True)

    df_filtered = df_filtered.copy()
    df_filtered["model"] = pd.Categorical(
        df_filtered["model"], categories=order_to_use, ordered=True
    )
    df_filtered = df_filtered.sort_values(["model", "variant"]).reset_index(drop=True)

    # Pivot by variant (so columns = V1, V2, V3, ...)
    df_variants = (
        df_filtered.pivot_table(
            index="model", columns="variant", values="price_lakhs", aggfunc="first"
        ).reindex(order_to_use)
    )
    variant_cols = list(df_variants.columns)

    # --- Merge tables ---
    merged_df = price_range_df.merge(
        df_variants.reset_index(), on="model", how="left"
    )

    # --- Create workbook & single sheet ---
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("Price_Range")

    # Formats
    fmt_header = workbook.add_format(
        {"bold": True, "bg_color": "#D9E1F2", "border": 1}
    )
    fmt_num = workbook.add_format({"num_format": "(0.00)", "border": 1})
    fmt_text = workbook.add_format({"border": 1})

    # --- Write merged table ---
    headers = ["Model", "Min Price (Lakh)", "Max Price (Lakh)", "Delta (Lakh)"] + variant_cols
    ws.write_row(0, 0, headers, fmt_header)

    for r, row in enumerate(merged_df.itertuples(index=False), start=1):
        ws.write(r, 0, row.model, fmt_text)
        ws.write_number(r, 1, float(row.min_price_lakh), fmt_num)
        ws.write_number(r, 2, float(row.max_price_lakh), fmt_num)
        # Delta = Max - Min
        ws.write_formula(r, 3, f"=C{r+1}-B{r+1}", fmt_num)
        # Variants
        for c, val in enumerate(row[3:], start=4):  # offset by 4 cols
            if pd.notna(val):
                ws.write_number(r, c, float(val), fmt_num)

    max_row = len(merged_df)

    # Autofit columns
    ws.set_column(0, 0, 15)
    ws.set_column(1, len(headers) - 1, 9)

    # --- Chart ---
    col_chart = workbook.add_chart({"type": "column", "subtype": "stacked"})

    # Min price transparent
    col_chart.add_series({
        "name": "Min Price",
        "categories": ["Price_Range", 1, 0, max_row, 0],
        "values": ["Price_Range", 1, 1, max_row, 1],
        "fill": {"none": True},
        "border": {"none": True},
        "line": {"none": True},
        "shadow": {"blur": 0},
        "gap": 200,
    })

    # Delta visible
    col_chart.add_series({
        "name": "Price Range",
        "categories": ["Price_Range", 1, 0, max_row, 0],
        "values": ["Price_Range", 1, 3, max_row, 3],
        "fill": {"color": "#ADD8E6"},
        "border": {"none": True},
        "gap": 200,
    })

    # Variants as markers
    line_chart = workbook.add_chart({"type": "line"})
    for idx_col, variant in enumerate(variant_cols, start=4):  # start at col 4
        line_chart.add_series({
            "name": variant,
            "categories": ["Price_Range", 1, 0, max_row, 0],
            "values": ["Price_Range", 1, idx_col, max_row, idx_col],
            "marker": {
                "type": "circle",
                "size": 7,
                "border": {"color": "white"},
                "fill": {"color": "#00008B"},
            },
            "line": {"none": True},
            "data_labels": {
                "value": True,
                "series_name": True,
                "category": False,
                "position": "right",
                "font": {"size": 10, "bold": True, "color": "#333333"},
                "separator": " ",
            },
        })

    col_chart.combine(line_chart)

    # --- Adjust Y axis scale ---
    max_price = price_range_df["max_price_lakh"].max()
    y_max = (max_price * 1.1)

    col_chart.set_title({
        "name": "Price Range per Model (₹ Lakhs)",
        "name_font": {"bold": True, "size": 12},
    })
    col_chart.set_x_axis({"name": "Model", "label_position": "low"})
    col_chart.set_y_axis({
        "name": "Price (₹ Lakhs)",
        "num_format": "₹0.0",
        "major_gridlines": {"visible": True, "line": {"color": "#D9D9D9"}},
        "min": 0,
        "max": y_max,
    })
    col_chart.set_legend({"none": True})
    col_chart.set_size({"width": 1000, "height": 520})
    col_chart.set_style(0)

    # Insert chart
    ws.insert_chart("F2", col_chart, {"x_scale": 1.5, "y_scale": 1.1})

    workbook.close()
    output.seek(0)
    return output.getvalue()



excel_data = to_excel_price_range_chart(df_filtered, order_to_use)
st.download_button(
    label="📥 Download Price Range Excel",
    data=excel_data,
    file_name="Price_Range_Chart.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
