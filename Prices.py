# streamlit_app.py
import requests
import streamlit as st
from datetime import datetime
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import re
import plotly.graph_objects as go
import plotly.express as px
import sqlite3
import textwrap
import pandas as pd
from datetime import date
import streamlit_sortables as sortables
# =====================
# CONFIG
# =====================
DB_FILE = "Old/prices.db"
CITY_CODE = "08"  # Maruti city code: 08 = Delhi
ARENA_CHANNELS = "NRM,NRC"
NEXA_CHANNEL = "EXC"

# =====================
# SESSION + HELPERS
# =====================
session = requests.Session()
retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def _parse_price_rupees(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(round(v))
    s = str(v).strip().replace(",", "").replace(" ", "").replace("₹", "")
    if not s:
        return None
    s = re.sub(r"(?i)Lakhs?|L$", "L", s)
    try:
        if s.lower().endswith("l"):
            return int(float(s[:-1]) * 100000)
        if s.lower().endswith("cr"):
            return int(float(s[:-2]) * 10000000)
        return int(float(s))
    except:
        return None


# =====================
# TATA SCRAPER
# =====================
TATA_MODEL_CONFIGS = [
    {
        "name": "Nexon",
        "url": "https://cars.tatamotors.com/nexon/ice/price.getpricefilteredresult.json",
        "modelId": "1-TGW7UPH",
        "parentProductId": "1-S3YJYTJ",
        "referer": "https://cars.tatamotors.com/nexon/ice/price.html"
    },
    {
        "name": "Tiago",
        "url": "https://cars.tatamotors.com/tiago/ice/price.getpricefilteredresult.json",
        "modelId": "1-FFSXOSX",
        "parentProductId": "1-DR4I0XM",
        "referer": "https://cars.tatamotors.com/tiago/ice/price.html"
    },
    {
        "name": "Altroz",
        "url": "https://cars.tatamotors.com/altroz/ice/price.getpricefilteredresult.json",
        "modelId": "1-1NR5UKP5",
        "parentProductId": "1-1MJPLCOH",
        "referer": "https://cars.tatamotors.com/altroz/ice/price.html"
    },
    {
        "name": "Curvv",
        "url": "https://cars.tatamotors.com/curvv/ice/price.getpricefilteredresult.json",
        "modelId": "CURVV_MODEL_ID",
        "parentProductId": "CURVV_PARENT_ID",
        "referer": "https://cars.tatamotors.com/curvv/ice/price.html"
    },
    {
        "name": "Tigor",
        "url": "https://cars.tatamotors.com/tigor/ice/price.getpricefilteredresult.json",
        "modelId": "TIGOR_MODEL_ID",
        "parentProductId": "TIGOR_PARENT_ID",
        "referer": "https://cars.tatamotors.com/tigor/ice/price.html"
    },
    {
        "name": "Punch",
        "url": "https://cars.tatamotors.com/punch/ice/price.getpricefilteredresult.json",
        "modelId": "PUNCH_MODEL_ID",
        "parentProductId": "PUNCH_PARENT_ID",
        "referer": "https://cars.tatamotors.com/punch/ice/price.html"
    },
    {
        "name": "Harrier",
        "url": "https://cars.tatamotors.com/harrier/ice/price.getpricefilteredresult.json",
        "modelId": "5-20YZDK9O",
        "parentProductId": "1-12DWLRE2",
        "referer": "https://cars.tatamotors.com/harrier/ice/price.html"
    },
    {
        "name": "Safari",
        "url": "https://cars.tatamotors.com/safari/ice/price.getpricefilteredresult.json",
        "modelId": "SAFARI_MODEL_ID",
        "parentProductId": "SAFARI_PARENT_ID",
        "referer": "https://cars.tatamotors.com/safari/ice/price.html"
    }
]

TATA_EDITION_LIST = ["standard"]
TATA_FUEL_LIST = ["1-D1MGNW9", "1-ID-1738", "1-ID-267","1-ID-268"]   # Petrol / Diesel / CNG
TATA_TRANS_LIST = ["5-251EY13B", "5-251EY13H", "5-251EY13J", "MT", "AMT", "DCA", "DCT"]
TATA_PRICE_RANGE = ["₹5L", "₹25L"]

FUEL_MAP = {"1-D1MGNW9": "Petrol", "1-ID-1738": "Diesel", "1-ID-267": "CNG"}
TRANS_MAP = {"5-251EY13B": "MT", "5-251EY13H": "AMT", "5-251EY13J": "DCT"}

TATA_HEADERS_TEMPLATE = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://cars.tatamotors.com",
    "referer": None,
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}
TATA_COOKIES = {"at_check": "true"}

def _tata_fetch_one(model_cfg, edition, fuel, trans):
    headers = TATA_HEADERS_TEMPLATE.copy()
    headers["referer"] = model_cfg["referer"]
    payload = {
        "vehicleCategory": "TMPC",
        "modelId": model_cfg["modelId"],
        "parentProductId": model_cfg["parentProductId"],
        "cityId": "India-DL-DELHI",
        "filtersSelected": [
            {"filterType": "fuel_type", "values": [fuel]},
            {"filterType": "transmission_type", "values": [trans]},
            {"filterType": "edition", "values": [edition]},
            {"filterType": "price", "values": TATA_PRICE_RANGE},
        ]
    }
    try:
        resp = session.post(model_cfg["url"], headers=headers, cookies=TATA_COOKIES, json=payload, timeout=20)
        data = resp.json()
    except:
        return []

    variants = data.get("results", {}).get("variantPriceFeatures", []) or []
    out = []
    for v in variants:
        price_raw = v.get("priceDetails", {}).get("originalPrice")
        price = _parse_price_rupees(price_raw)
        if not price:
            continue
        out.append({
            "Brand": "Tata",
            "Model": model_cfg["name"],
            "Fuel": FUEL_MAP.get(fuel, fuel),
            "Transmission": TRANS_MAP.get(trans, trans),
            "Variant": v.get("variantLabel", ""),
            "Price": price
        })
    return out

def fetch_tata_prices_parallel():
    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [
            ex.submit(_tata_fetch_one, cfg, edition, fuel, trans)
            for cfg in TATA_MODEL_CONFIGS
            for edition, fuel, trans in itertools.product(TATA_EDITION_LIST, TATA_FUEL_LIST, TATA_TRANS_LIST)
        ]
        for f in as_completed(futures):
            r = f.result()
            if r:
                rows.extend(r)
    return rows

# =====================
# MARUTI SCRAPER (parallel by model)
# =====================
def _maruti_fetch_arena_model(modelCd, modelName):
    VARIANT_URL = f"https://www.marutisuzuki.com/graphql/execute.json/msil-platform/arenaVariantList;modelCd={modelCd}"
    PRICE_URL = "https://www.marutisuzuki.com/pricing/v2/common/pricing/ex-showroom-detail"
    rows = []
    try:
        var_res = session.get(VARIANT_URL, timeout=15)
        variants = var_res.json().get("data", {}).get("carVariantList", {}).get("items", [])
        if not variants:
            return rows

        params = {
            "forCode": CITY_CODE,
            "modelCodes": modelCd,
            "channel": ARENA_CHANNELS,
            "variantInfoRequired": "true"
        }
        price_res = session.get(PRICE_URL, params=params, timeout=15)
        price_map = {
            v["variantCd"]: int(round(v["exShowroomPrice"]))
            for m in price_res.json().get("data", {}).get("models", [])
            for v in m.get("exShowroomDetailResponseDTOList", [])
            if v.get("colorType") == "M"
        }

        for v in variants:
            price = price_map.get(v["variantCd"])
            if price:
                rows.append({
                    "Brand": "Maruti",
                    "Model": modelName,
                    "Fuel": v.get("fuelType", ""),
                    "Transmission": v.get("transmission", ""),
                    "Variant": re.sub(r"\b(CNG|MT|AGS|AMT|AT)\b", "",
                                      v.get("variantName", "").replace(modelName, "")).strip(),
                    "Price": price
                })
    except:
        pass
    return rows

def _maruti_fetch_nexa_model(modelCd, modelName):
    ACTIVE_VARIANTS_URL = f"https://www.nexaexperience.com/graphql/execute.json/msil-platform/VariantFeaturesList;modelCd={modelCd};locale=en;"
    PRICES_URL = "https://www.nexaexperience.com/pricing/v2/common/pricing/ex-showroom-detail"
    rows = []
    try:
        variants_data = session.get(ACTIVE_VARIANTS_URL, timeout=20).json()
        prices_data = session.get(PRICES_URL, params={
            "forCode": CITY_CODE,
            "channel": NEXA_CHANNEL,
            "variantInfoRequired": "true"
        }, timeout=20).json()

        variant_prices = {
            var["variantCd"]: var["exShowroomPrice"]
            for model in prices_data.get("data", {}).get("models", [])
            for var in model.get("exShowroomDetailResponseDTOList", [])
        }

        for car_model in variants_data.get("data", {}).get("carModelList", {}).get("items", []):
            for variant in car_model.get("variants", []):
                price = variant_prices.get(variant.get("variantCd"))
                # Extract transmission type
                transmission = variant.get("transmission", "")
                # Remove spaces
                transmission = transmission.replace(" ", "")

                # Normalize common cases
                if "MT" in transmission:  # e.g. 5MT, 6MT
                    transmission = "Manual"
                elif "AT" in transmission:  # e.g. 4AT, 6AT
                    transmission = "Automatic"

                rows.append({
                    "Brand": "Maruti",
                    "Model": modelName,
                    "Fuel": variant.get("fuelType", ""),
                    "Transmission": transmission,
                    "Variant": re.sub(r"\b(CNG|MT|AGS|AMT|AT)\b", "",
                                      variant.get("variantName", "").replace(modelName, "")).strip(),
                    "Price": int(round(price))
                })
    except:
        pass
    return rows

def fetch_maruti_prices_parallel():
    arena_models = {
        "DE": "Dzire", "AT": "Alto K10", "VZ": "Brezza", "SI": "Swift",
        "CL": "Celerio", "WA": "WagonR", "VR": "Eeco", "ER": "Ertiga", "SP": "S-Presso"
    }
    nexa_models = {
        "BZ": "Baleno", "CI": "Ciaz", "FR": "Fronx", "GV": "Grand Vitara",
        "IG": "Ignis", "IN": "Invicto", "JM": "Jimny", "XL": "XL6"
    }
    rows = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = []
        for cd, name in arena_models.items():
            futures.append(ex.submit(_maruti_fetch_arena_model, cd, name))
        for cd, name in nexa_models.items():
            futures.append(ex.submit(_maruti_fetch_nexa_model, cd, name))

        for f in as_completed(futures):
            r = f.result()
            if r:
                rows.extend(r)
    return rows


# =====================
# HYUNDAI SCRAPER (parallel by model)
# =====================

# ---------- HYUNDAI ----------
HYUNDAI_BASE_URL = "https://api.hyundai.co.in/service/price/getPriceByModelAndCity"
HYUNDAI_HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9,hi-IN;q=0.8,hi;q=0.7",
    "origin": "https://www.hyundai.com",
    "priority": "u=1, i",
    "referer": "https://www.hyundai.com/",
    "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
}
# Delhi cityId=1370 (you can add more cities later)
HYUNDAI_MODELS = [
    {"cityId": 1370, "modelId": 24, "modelName": "Grand i10 NIOS"},
    {"cityId": 1370, "modelId": 39, "modelName": "i20"},
    {"cityId": 1370, "modelId": 41, "modelName": "i20 N Line"},
    {"cityId": 1370, "modelId": 35, "modelName": "AURA"},
    {"cityId": 1370, "modelId": 45, "modelName": "Verna"},
    {"cityId": 1370, "modelId": 18, "modelName": "Venue"},
    {"cityId": 1370, "modelId": 37, "modelName": "Creta"},
    {"cityId": 1370, "modelId": 40, "modelName": "Alcazar"},
    {"cityId": 1370, "modelId": 46, "modelName": "EXTER"},
    {"cityId": 1370, "modelId": 42, "modelName": "Tucson"},
    {"cityId": 1370, "modelId": 43, "modelName": "Venue N Line"},
    {"cityId": 1370, "modelId": 47, "modelName": "Creta N Line"},
    {"cityId": 1370, "modelId": 48, "modelName": "Creta Electric"},
]
def _hyundai_fetch_one(model):
    params = {
        "cityId": model["cityId"],
        "modelId": model["modelId"],
        "loc": "IN",
        "lan": "en"
    }
    rows = []
    try:
        r = session.get(HYUNDAI_BASE_URL, headers=HYUNDAI_HEADERS, params=params, timeout=20)
        if r.status_code != 200:
            return rows
        data = r.json()
        # Some endpoints return list; some return dict with "modelPrice"
        variants = []
        if isinstance(data, dict) and "modelPrice" in data:
            variants = data["modelPrice"] or []
        elif isinstance(data, list):
            variants = data
        else:
            variants = []

        for v in variants:
            price_rupees = _parse_price_rupees(v.get("price"))
            fuel = v.get("fuelType", "")
            if "CNG" in fuel:  # covers "Bi-Fuel CNG", "CNG", etc.
                fuel = "CNG"
            transmission = v.get("transmission", "").replace(" ", "")  # remove spaces

            # Normalize transmission types
            if any(x in transmission for x in ["AT", "DCT", "IVT", "AMT", "Automatic"]):
                transmission = "Automatic"
            elif "MT" in transmission or "Manual" in transmission:
                transmission = "Manual"

            if not price_rupees:
                continue
            rows.append({
                "Brand": "Hyundai",
                "Model": model["modelName"],
                "Fuel": fuel,
                "Transmission": transmission,
                "Variant": (
                    v.get("variant", "Unknown")
                    .replace(model["modelName"], "")  # remove model name
                    .replace("-", " ")  # replace hyphens with space
                    .replace(model["modelName"].upper(),"")
                    .strip()  # remove extra spaces
                ),

                "Price": price_rupees
            })
    except:
        pass
    return rows

def fetch_hyundai_prices_parallel():
    rows = []
    with ThreadPoolExecutor(max_workers=14) as ex:
        futures = [ex.submit(_hyundai_fetch_one, m) for m in HYUNDAI_MODELS]
        for f in as_completed(futures):
            r = f.result()
            if r:
                rows.extend(r)
    return rows

# =====================
# MAHINDRA SCRAPER
# =====================
MAHINDRA_MODELS = [
    {"name": "Thar ROXX", "pid": "TH5D", "colorCode": "A3DPFRSMBK"},
    {"name": "XUV 3XO", "pid": "X3XO", "colorCode": "A3CTNYLOBK"},
    {"name": "Thar ROXX", "pid": "TH5D", "colorCode": "A3DPFRSMBK"},
    {"name": "XUV 3XO", "pid": "X3XO", "colorCode": "A3CTNYLOBK"},
    {"name": "XUV700", "pid": "X700M063917795233", "colorCode": "A3XXXXX"},  # Replace with actual
    {"name": "SCORPIO-N", "pid": "SCN", "colorCode": "A3XXXXX"},
    {"name": "SCORPIO CLASSIC", "pid": "SCRC", "colorCode": "A3XXXXX"},
    {"name": "BOLERO NEO", "pid": "NEO", "colorCode": "A3XXXXX"},
    {"name": "BOLERO", "pid": "BOL", "colorCode": "A3XXXXX"},
    {"name": "XUV400", "pid": "X400", "colorCode": "A3XXXXX"},
    {"name": "MARAZZO", "pid": "MRZO", "colorCode": "A3XXXXX"},
    {"name": "VEERO", "pid": "VEERO", "colorCode": "A3XXXXX"},
    # Add other models with actual color codes here
]

MAHINDRA_BASE_URL = "https://auto.mahindra.com/on/demandware.store/Sites-amc-Site/en_IN/Product-Variation"

def _mahindra_fetch_one(model):
    color_param_name = f"dwvar_{model['pid']}_colorCode"
    params = {
        color_param_name: model["colorCode"],
        "pid": model["pid"],
        "quantity": 1
    }
    try:
        resp = session.get(MAHINDRA_BASE_URL, params=params, timeout=20)
        data = resp.json()
    except Exception as e:
        print(f"Failed to fetch {model['name']}: {e}")
        return []

    variant_html_list = data.get("product", {}).get("variantCardHtml", [])
    rows = []

    for html_snippet in variant_html_list:
        soup = BeautifulSoup(html_snippet, "html.parser")
        input_tag = soup.find("input", {"class": "js-radio"})
        variant_name = (
            input_tag.attrs.get("data-variantName") or
            input_tag.attrs.get("data-variantname") or
            "N/A"
        )
        price_tag = soup.find("span", {"class": "approx-price"})
        price_text = price_tag.text.strip() if price_tag else "N/A"
        price_int = _parse_price_rupees(price_text)

        rows.append({
            "Brand": "Mahindra",
            "Model": model["name"],
            "Fuel": "",
            "Transmission": "",
            "Variant": variant_name,
            "Price": price_int,
        })

    return rows

def fetch_mahindra_prices_parallel():
    rows = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_mahindra_fetch_one, m) for m in MAHINDRA_MODELS]
        for f in as_completed(futures):
            result = f.result()
            if result:
                rows.extend(result)
    return rows

# =====================
# DB HELPERS
# =====================
def init_db():
    connection = sqlite3.connect(DB_FILE)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            brand TEXT,
            model TEXT,
            fuel TEXT,
            transmission TEXT,
            variant TEXT,
            price INTEGER,
            source TEXT DEFAULT 'scraped'
        )
    """)
    connection.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON prices(timestamp)")
    connection.commit()
    connection.close()

def store_prices(prices):
    if not prices:
        return
    conn = sqlite3.connect(DB_FILE)
    now = datetime.now().isoformat()
    conn.executemany("""
        INSERT INTO prices (timestamp, brand, model, fuel, transmission, variant, price)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        (now, r["Brand"], r["Model"], r["Fuel"], r["Transmission"], r["Variant"], r["Price"])
        for r in prices
    ])
    conn.commit()
    conn.close()

def get_latest_prices():
    conn = sqlite3.connect(DB_FILE)
    q = """
        SELECT brand, model, fuel, transmission, variant, price, source, timestamp
        FROM prices
        WHERE source='manual'
        OR timestamp = (
            SELECT MAX(timestamp) FROM prices WHERE source='scraped'
        )
    """
    df = pd.read_sql_query(q, conn)
    conn.close()
    return df


# =====================
# MASTER SCRAPER (button triggers calls)
# =====================
def scrape_all_brands_parallel():
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_maruti = ex.submit(fetch_maruti_prices_parallel)
        f_tata = ex.submit(fetch_tata_prices_parallel)
        f_hyundai = ex.submit(fetch_hyundai_prices_parallel)
        f_mahindra = ex.submit(fetch_mahindra_prices_parallel)
        maruti = f_maruti.result()
        tata = f_tata.result()
        hyundai = f_hyundai.result()
        mahindra = f_mahindra.result()

    all_prices = (maruti or []) + (tata or []) + (hyundai or [])+ (mahindra or [])
    return all_prices

def add_price(brand, model, variant, price, fuel, transmission):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO prices (brand, model, variant, price, fuel, transmission, timestamp, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'manual')
    """, (brand, model, variant, price, fuel, transmission, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def delete_price(record_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM prices WHERE id = ? AND source='manual'", (record_id,))
    conn.commit()
    conn.close()

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
        .stButton>button { background-color: #f0f0f0; color: #000000; border-radius: 8px; }
        .js-plotly-plot .plotly { background-color: #FFFFFF !important; }
        </style>
        """
        return custom_css, "#FFFFFF", "black"
    else:
        custom_css = """
        <style>
        .stApp { background-color: #181818; color: #FFFFFF; }
        section[data-testid="stSidebar"] { background-color: #262626; color: #FFFFFF; }
        .stMultiSelect div[data-baseweb="tag"] { background-color: #444444; color: #FFFFFF; }
        .stButton>button { background-color: #333333; color: #FFFFFF; border-radius: 8px; }
        .js-plotly-plot .plotly { background-color: #181818 !important; }
        </style>
        """
        return custom_css, "#000000", "white"

# 🔘 Toggle (default = light mode False → dark theme)
light_mode = st.toggle("🌞 Light Mode", value=False)

# 🎨 Apply theme
custom_css, plot_bgcolor, font_color = apply_theme(light_mode)
st.markdown(custom_css, unsafe_allow_html=True)

# =====================
# STREAMLIT APP
# =====================
st.set_page_config(page_title="Car Price Dashboard", layout="wide")
st.title("🚗 Car Price Dashboard")
init_db()

if st.button("🔄 Fetch Latest Prices"):
    with st.spinner("Calling brand APIs in parallel..."):
        scraped = scrape_all_brands_parallel()
        if scraped:
            store_prices(scraped)
            st.success(f"Scraped & stored {len(scraped)} records.")
        else:
            st.error("No prices scraped.")

df = get_latest_prices()
if df.empty:
    st.info("No data yet. Click **Fetch Latest Prices** to load.")
    st.stop()

# Ensure price is numeric
df["price"] = pd.to_numeric(df["price"], errors="coerce")
df["price_lakhs"] = (df["price"] / 100000).round(2)

# =====================
# Dynamic & Searchable Filters
# =====================
st.sidebar.header("Filter Options")

# Brand filter
brands_available = sorted(df["brand"].unique())
selected_brands = st.sidebar.multiselect(
    "Brand(s)", options=brands_available, default=[]
)

models_available = sorted(df[df["brand"].isin(selected_brands)]["model"].unique())
selected_models = st.sidebar.multiselect(
    "Model(s)", options=models_available, default=models_available
)

# Fuel filter depends on selected brands & models
fuel_available = sorted(df[
    df["brand"].isin(selected_brands) & df["model"].isin(selected_models)
]["fuel"].unique())
selected_fuel = st.sidebar.multiselect(
    "Fuel(s)", options=fuel_available, default=fuel_available
)

# Transmission filter depends on selected brands & models
trans_available = sorted(df[
    df["brand"].isin(selected_brands) & df["model"].isin(selected_models)
]["transmission"].unique())
selected_trans = st.sidebar.multiselect(
    "Transmission(s)", options=trans_available, default=trans_available
)

st.sidebar.subheader("➕ Add Variant Price")

# Sidebar manual entry form
with st.sidebar.form("price_entry_form", clear_on_submit=True):
    brand_in = st.text_input("Brand", value="Maruti")
    model_in = st.text_input("Model")
    variant_in = st.text_input("Variant")
    fuel_in = st.text_input("Fuel", value="Petrol")
    trans_in = st.text_input("Transmission", value="Manual")

    # 👇 Input price in Lakhs, store in rupees
    price_lakh_in = st.number_input("Price (₹ Lakhs)", min_value=0.0, step=0.1, format="%.2f")

    submitted = st.form_submit_button("Add Price")
    if submitted and brand_in and model_in and variant_in and price_lakh_in > 0:
        price_rupees = int(price_lakh_in * 100000)  # convert lakhs → rupees
        add_price(brand_in, model_in, variant_in, price_rupees, fuel_in, trans_in)
        st.success(f"✅ Added {brand_in} {model_in} {variant_in} {fuel_in} {trans_in} at ₹{price_rupees:,.0f}")
        st.rerun()

st.subheader("🗑️ Delete a Manual Entry")

conn = sqlite3.connect(DB_FILE)
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
        delete_price(record_choice.id)   # ✅ deletes only manual
        st.success("✅ Manual entry deleted.")
        st.rerun()



# Price slider filter
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
# Variant Label Filter
# =====================
all_variants = df_filtered["variant"].unique().tolist()
selected_variants = st.multiselect(
    "Select Variants to Show in Chart Labels",
    options=all_variants,
    default=all_variants
)

# Only selected variants get a label
df_filtered["label"] = df_filtered.apply(
    lambda r: f"{r['variant']} {'CNG' if 'cng' in str(r['fuel']).lower() else ''} ({r['price_lakhs']:.2f}L)"
    if r["variant"] in selected_variants else "",
    axis=1
)


# =====================
# Chart selection
# =====================
chart_type = st.radio(
    "Select Chart Type",
    ["Price Range Chart","Scatter Plot", "Violin Plot", "Line Chart", "Treemap"],  # NEW OPTION
    horizontal=True
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

# ---------------------
# Price Range Chart
# ---------------------
if chart_type == "Price Range Chart":
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
        hoverinfo="skip"
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
            f"{r['variant']} {'CNG' if r['fuel'] == 'CNG' else ''}", width=12
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
    title="Car Model Price Ranges and Variant Prices (in Lakhs)",
    xaxis=dict(
        title="Model",
        tickangle=0,
        automargin=True,
        rangeslider=dict(visible=False),
        fixedrange=False
    ),
    yaxis=dict(
        title="Price (₹ Lakhs)",
        automargin=True,
        fixedrange=False
    ),
    hovermode="closest",
    plot_bgcolor=plot_bgcolor,
    paper_bgcolor=plot_bgcolor,
    font=dict(color=font_color)
)

st.plotly_chart(fig, use_container_width=True)


# Price Table
st.subheader("Price Table (sorted by Brand, Model, Price)")
df_table = df_filtered.sort_values(["brand", "model", "price_lakhs"])[
    ["brand", "model", "fuel", "transmission", "variant", "price_lakhs"]
].rename(columns={"price_lakhs": "Price (₹ Lakhs)"})
df_table["Price (₹ Lakhs)"] = df_table["Price (₹ Lakhs)"].map(lambda x: f"{x:.2f}")
st.dataframe(df_table, use_container_width=True)


def load_price_history():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM prices ORDER BY timestamp", conn)
    conn.close()
    return df

st.title("📈 Price History Viewer")

df = load_price_history()

if df.empty:
    st.warning("No price history found in the database.")
else:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    # 👉 Add this line to convert price into lakhs
    df["price_lakhs"] = df["price"] / 100000

    df["date"] = df["timestamp"].dt.date

    # Sidebar filters
    brand = st.selectbox("Select Brand", sorted(df["brand"].unique()))
    models = st.multiselect(
        "Select Models",
        sorted(df[df["brand"] == brand]["model"].unique()),
        default=df[df["brand"] == brand]["model"].unique()
    )
    variants = st.multiselect(
        "Select Variants",
        sorted(df[(df["brand"] == brand) & (df["model"].isin(models))]["variant"].unique()),
        default=df[(df["brand"] == brand) & (df["model"].isin(models))]["variant"].unique()
    )

    # Filter dataframe
    df_filtered = df[
        (df["brand"] == brand) &
        (df["model"].isin(models)) &
        (df["variant"].isin(variants))
    ].copy()

    if df_filtered.empty:
        st.warning("No data for selected combination.")
    else:
        # Aggregate by date and variant
        df_daywise = (
            df_filtered.groupby(["model", "variant", "date"], observed=True)
            .agg({"price": "last"})
            .reset_index()
        )

        # Fill missing dates for each model+variant
        # Ensure there is at least one valid date
        if pd.isna(df_daywise["date"].min()):
            st.warning("No valid date found for the selected data.")
        else:
            all_days = pd.date_range(start=df_daywise["date"].min(), end=date.today(), freq="D")
            filled_list = []
            for (mdl, var) in df_daywise[["model", "variant"]].drop_duplicates().itertuples(index=False):
                temp = (
                    df_daywise[(df_daywise["model"] == mdl) & (df_daywise["variant"] == var)]
                    .set_index("date")
                    .reindex(all_days)
                    .rename_axis("date")
                    .reset_index()
                )
                temp["model"] = mdl
                temp["variant"] = var
                temp["price"] = temp["price"].ffill()
                filled_list.append(temp)

            df_daywise = pd.concat(filled_list, ignore_index=True)
            df_daywise["price_lakhs"] = (df_daywise["price"] / 100000).round(2)

            # Create a unique label for color separation
            df_daywise["label"] = df_daywise["model"] + " - " + df_daywise["variant"]

        # Plot interactive chart
        fig = px.line(
            df_daywise,
            x="date",
            y="price_lakhs",
            color="label",
            title=f"Price Trend: {brand}",
            markers=True
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Price (₹ Lakhs)"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Show full price history in wide format
        st.subheader("📜 Price History")

        df_wide = df_daywise.pivot_table(
            index=["model", "variant"],  # Keep one row per model+variant
            columns="date",  # Dates become columns
            values="price_lakhs",  # Show price in lakhs
            aggfunc="last"  # Last price of the day
        ).reset_index()

        # Optional: add brand if needed
        df_wide.insert(0, "brand", brand)

        # Format column names for display
        df_wide.columns = [str(c) for c in df_wide.columns]

        st.dataframe(df_wide, use_container_width=True)
