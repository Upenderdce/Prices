import requests
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import asyncio
import aiohttp
import itertools

def remove_duplicates(prices_list):
    df = pd.DataFrame(prices_list)
    df = df.drop_duplicates(subset=["Brand", "Model", "Variant", "Fuel", "Transmission", "Price"])
    df = df.reset_index(drop=True)
    return df.to_dict(orient="records")

# =====================
# CONFIG
# =====================

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

# =============================
# Config
# =============================
TATA_MODEL_CONFIGS = [
    {"name": "Nexon", "modelId": "1-TGW7UPH", "parentProductId": "1-S3YJYTJ", "baseUrl": "https://cars.tatamotors.com/nexon/ice"},
    {"name": "Tiago", "modelId": "1-FFSXOSX", "parentProductId": "1-DR4I0XM", "baseUrl": "https://cars.tatamotors.com/tiago/ice"},
    {"name": "Altroz", "modelId": "1-1NR5UKP5", "parentProductId": "1-1MJPLCOH", "baseUrl": "https://cars.tatamotors.com/altroz/ice"},
    {"name": "Harrier", "modelId": "5-20YZDK9O", "parentProductId": "1-12DWLRE2", "baseUrl": "https://cars.tatamotors.com/harrier/ice"},
    {"name": "Curvv", "modelId": "1-1A9U7P1Z", "parentProductId": "1-1U4U0U0N", "baseUrl": "https://cars.tatamotors.com/curvv/ice"},
    {"name": "Safari", "modelId": "5-20YWEGYC", "parentProductId": "1-12DXM1K4", "baseUrl": "https://cars.tatamotors.com/safari/ice"},
    {"name": "Punch", "modelId": "1-11Z2ID06", "parentProductId": "1-13T1XGN8", "baseUrl": "https://cars.tatamotors.com/punch/ice"},
    {"name": "Tigor", "modelId": "1-13LP1VGC", "parentProductId": "1-13LP1VGE", "baseUrl": "https://cars.tatamotors.com/tigor/ice"},
]


TATA_EDITION_LIST = ["standard"]
TATA_PRICE_RANGE = ["₹5L", "₹30L"]

FUEL_MAP = {"1-D1MGNW9": "CNG", "1-ID-1738": "Diesel", "1-ID-267": "Petrol", "1-ID-268": "CNG"}
TRANS_MAP = {"5-251EY13B": "MT", "5-251EY13H": "AMT", "5-251EY13J": "AT", "DCA": "AT", "DCT": "AT"}

TATA_HEADERS_TEMPLATE = {
    "accept": "*/*",
    "origin": "https://cars.tatamotors.com",
    "referer": None,
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}
TATA_COOKIES = {"at_check": "true"}

session = requests.Session()
FILTER_CACHE = {}  # cache filters per model

# =============================
# Helpers
# =============================
def _parse_tata_price_rupees(price_str):
    if not price_str:
        return None
    digits = re.sub(r"[^\d]", "", price_str)
    return int(digits) if digits else None

def _clean_variant_name(model_name, raw_name):
    v = (raw_name or "").replace(model_name, "").replace(model_name.upper(), "")
    v = v.replace("-", " ")
    for fuel_word in ["Petrol/Ethanol", "Petrol", "Diesel", "PETROL", "DIESEL"]:
        v = v.replace(fuel_word, "")
    v = re.sub(r"\b(5MT|MT|Standard|New)\b", "", v)
    v = re.sub(r"\b(Bi[- ]?fuel.*|BIFUEL.*)\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"(,\s*)?CNG\b", " CNG", v, flags=re.IGNORECASE)
    v = re.sub(r"\b(CNG)(\s*CNG)+\b", r"\1", v, flags=re.IGNORECASE)
    v = re.sub(r"\s+,", ",", v)
    v = re.sub(r"\s{2,}", " ", v)
    return v.strip(" ,")

# =============================
# Fetch filter options (cached)
# =============================
def _tata_get_filters(model_cfg):
    if model_cfg["name"] in FILTER_CACHE:
        return FILTER_CACHE[model_cfg["name"]]

    url = f"{model_cfg['baseUrl']}/price.getpricefilteroptions.json"
    headers = TATA_HEADERS_TEMPLATE.copy()
    headers["referer"] = f"{model_cfg['baseUrl']}/price.html"
    headers["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"

    payload = {
        "vehicleCategory": "TMPC",
        "modelId": model_cfg["modelId"],
        "parentProductId": model_cfg["parentProductId"],
        "cityId": "India-DL-DELHI"
    }

    resp = session.post(url, headers=headers, cookies=TATA_COOKIES, data=payload, timeout=12)
    resp.raise_for_status()
    data = resp.json()

    filter_map = {"fuel_type": {}, "transmission_type": {}, "edition": {}}
    for opt in data.get("results", {}).get("filterOptionsList", []):
        ftype = opt.get("filterType")
        if ftype in filter_map:
            for item in opt.get("filterOption", []):
                filter_map[ftype][item["optionId"]] = item["optionLabel"]

    FILTER_CACHE[model_cfg["name"]] = filter_map
    return filter_map


# =============================
# Fetch prices for one combo
# =============================
def _tata_fetch_one(model_cfg, edition, fuel, trans):
    headers = TATA_HEADERS_TEMPLATE.copy()
    headers["referer"] = f"{model_cfg['baseUrl']}/price.html"
    headers["content-type"] = "application/json"

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
        url = f"{model_cfg['baseUrl']}/price.getpricefilteredresult.json"
        resp = session.post(url, headers=headers, cookies=TATA_COOKIES, json=payload, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"Error fetching {model_cfg['name']} {fuel}/{trans}: {e}")
        return []

    variants = data.get("results", {}).get("variantPriceFeatures", []) or []
    out = []
    for v in variants:
        price = _parse_price_rupees(v.get("priceDetails", {}).get("originalPrice"))
        if not price:
            continue

        variant_name = _clean_variant_name(model_cfg["name"], v.get("variantLabel", ""))

        out.append({
            "Brand": "Tata",
            "Model": model_cfg["name"],
            "Fuel": FUEL_MAP.get(fuel, fuel),
            "Transmission": TRANS_MAP.get(trans, trans),
            "Variant": variant_name,
            "Price": price
        })

    return out
# =============================
# Main parallel fetch
# =============================
def fetch_tata_prices_parallel():
    rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = []
        for cfg in TATA_MODEL_CONFIGS:
            filter_map = _tata_get_filters(cfg)
            fuels = list(filter_map["fuel_type"].keys())
            trans = list(filter_map["transmission_type"].keys())
            editions = list(filter_map["edition"].keys()) or TATA_EDITION_LIST

            for edition, fuel, tran in itertools.product(editions, fuels, trans):
                futures.append(ex.submit(_tata_fetch_one, cfg, edition, fuel, tran))

        for f in as_completed(futures):
            r = f.result()
            if r:
                rows.extend(r)
    return rows

# =====================
# MARUTI SCRAPER (parallel by model)
# =====================
CITY_CODE = "08"  # Maruti city code: 08 = Delhi
ARENA_CHANNELS = "NRM,NRC"
VARIANT_URL = f"https://www.marutisuzuki.com/graphql/execute.json/msil-platform/arenaVariantList"
PRICE_URL = "https://www.marutisuzuki.com/pricing/v2/common/pricing/ex-showroom-detail"
PLACEHOLDER_URL = "https://www.marutisuzuki.com/placeholders.json"
def fetch_placeholders():
    try:
        res = session.get(PLACEHOLDER_URL, timeout=15)
        data = res.json().get("data", [])
        price_str = next((d["Text"] for d in data if "prices" in d["Key"].lower()), "")
        # Convert "VARIANT:PRICE,..." → dict
        price_map = {
            k: int(v)
            for item in price_str.split(",")
            if ":" in item
            for k, v in [item.split(":", 1)]
        }

        return price_map
    except Exception as e:
        print(f"❌ Error fetching placeholders: {e}")
        return {}
PLACEHOLDER_PRICES = fetch_placeholders()

def _maruti_fetch_arena_model(modelCd, modelName):
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
                fuel = v.get("fuelType", "")
                if fuel.lower() in ["strong-hybrid"]:
                    fuel = "Petrol"
                rows.append({
                    "Brand": "Maruti",
                    "Model": modelName,
                    "Fuel": fuel,
                    "Transmission": v.get("transmission", ""),
                    "Variant": re.sub(r"\b(5MT|MT)\b", "",
                                      v.get("variantName", "").replace(modelName, "").replace("AGS", "AMT")).strip(),
                    "Price": price
                })
    except Exception as e:
        print(f"❌ Error fetching Maruti Arena model {modelName}: {e}")
    return rows

NEXA_CHANNEL = "EXC"
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
                fuel = variant.get("fuelType", "")
                if fuel.lower() in ["strong-hybrid"]:
                    fuel = "Petrol"
                rows.append({
                    "Brand": "Maruti",
                    "Model": modelName,
                    "Fuel": fuel,
                    "Transmission": transmission,
                    "Variant": re.sub(r"\b(5MT|MT)\b", "",
                                      variant.get("variantName", "").replace(modelName, "").replace("AGS", "AMT")).strip(),
                    "Price": int(round(price))
                })
    except Exception as e:
        print(f"❌ Error fetching Maruti Nexa model {modelName}: {e}")
    return rows

def fetch_maruti_prices_parallel():
    arena_models = {
        "DE": "Dzire", "AT": "Alto K10", "VZ": "Brezza", "SI": "Swift",
        "CL": "Celerio", "WA": "WagonR", "VR": "Eeco", "ER": "Ertiga", "SP": "S-Presso", "EC": "victoris"
    }
    nexa_models = {
        "BZ": "Baleno", "CI": "Ciaz", "FR": "Fronx", "GV": "Grand Vitara",
        "IG": "Ignis", "IN": "Invicto", "JM": "Jimny", "XL": "XL6"
    }
    rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
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
            variant_name = (
                v.get("variant", "Unknown")
                .replace(model["modelName"], "")  # remove model name
                .replace("-", " ")  # replace hyphens with space
                .replace(model["modelName"].upper(), "")
                .strip()  # remove extra spaces
            )

            # If edition is Knight, add it
            edition = v.get("edition")
            if edition:
                variant_name = f"{variant_name} {edition}"


            if not price_rupees:
                continue
            rows.append({
                "Brand": "Hyundai",
                "Model": model["modelName"],
                "Fuel": fuel,
                "Transmission": transmission,
                "Variant": variant_name,
                "Price": price_rupees
            })
    except Exception as e:
        print(f"❌ Error fetching Hyundai model {model['modelName']}: {e}")
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
        fuel = ""
        transmission = ""

        # Detect fuel
        if re.search(r"\bD\b", variant_name, flags=re.IGNORECASE):
            fuel = "Diesel"
        elif re.search(r"\bP\b", variant_name, flags=re.IGNORECASE):
            fuel = "Petrol"

        if re.search(r"\bDiesel\b", variant_name, flags=re.IGNORECASE) or re.search(r"\bD\b", variant_name):
            fuel = "Diesel"
        elif re.search(r"\bPetrol\b", variant_name, flags=re.IGNORECASE) or re.search(r"\bP\b", variant_name):
            fuel = "Petrol"

        # Detect transmission
        if re.search(r"\bAT\b", variant_name, flags=re.IGNORECASE):
            transmission = "Automatic"
        elif re.search(r"\bMT\b", variant_name, flags=re.IGNORECASE):
            transmission = "Manual"

        variant_name = re.sub(r'\b(Petrol|Diesel|CNG|EV|Hybrid)\b', '', variant_name, flags=re.I)
        variant_name = variant_name.strip()
        rows.append({
            "Brand": "Mahindra",
            "Model": model["name"],
            "Fuel": fuel,
            "Transmission": transmission,
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
# TOYOTA SCRAPER
# =====================
TOYOTA_BASE_URL = "https://webapi.toyotabharat.com/1.0/api/price"

TOYOTA_HEADERS = {
    "accept": "application/xml, text/xml, */*; q=0.01",
    "origin": "https://www.toyotabharat.com",
    "referer": "https://www.toyotabharat.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/139.0.0.0 Safari/537.36"
}
def normalize_toyota_fuel(fuel: str) -> str:
    if not fuel:
        return "NA"
    fuel = fuel.upper().strip()
    if fuel == "C":
        return "CNG"
    if fuel == "P":
        return "Petrol"
    if fuel == "D":
        return "Diesel"
    if fuel == "H":
        return "Hybrid"
    if fuel in ["E", "EV"]:
        return "EV"
    return fuel.title()


# ----------------------------
# Function 1: Fetch all models
# ----------------------------
def fetch_toyota_models():
    """Fetch all Toyota models (id + name)."""
    url = f"{TOYOTA_BASE_URL}/models"
    resp = requests.post(url, headers=TOYOTA_HEADERS, data="")
    soup = BeautifulSoup(resp.text, "xml")

    models = []
    for m in soup.find_all("PriceModel"):
        models.append({
            "id": m.find("Id").text.strip(),
            "name": m.find("Name").text.strip()
        })
    return models

# ----------------------------
# Function 2: Fetch prices for one model
# ----------------------------
def _toyota_prices(dealer_id, model_id, model_name):
    """Fetch price details for a given dealer & model."""
    url = f"{TOYOTA_BASE_URL}/list/{dealer_id}/{model_id}"
    resp = requests.post(url, headers=TOYOTA_HEADERS, data="")
    soup = BeautifulSoup(resp.text, "xml")

    rows = []
    for p in soup.find_all("Price"):
        grade = p.find("PriceGrade")

        if grade:
            # get the *variant name* only from direct children of <PriceGrade>
            variant_tag = grade.find("Name", recursive=False)
            variant = variant_tag.text.strip() if variant_tag else ""
            variant = re.sub(r"\b2WD \b", "", variant)
            variant=re.sub(r"\[.*?]", "", variant)

            fuel_tag = grade.find("FuelType", recursive=False)
            fuel = fuel_tag.text.strip() if fuel_tag else ""

            trans_tag = grade.find("Details", recursive=False)
            trans = trans_tag.text.strip() if trans_tag else ""
        else:
            variant, fuel, trans = "", "", ""

        amount_tag = p.find("Amount")
        amount = int(amount_tag.text.strip()) if amount_tag and amount_tag.text.strip().isdigit() else None

        rows.append({
            "Brand": "Toyota",
            "Model": model_name,
            "Fuel": normalize_toyota_fuel(fuel),
            "Transmission": trans,
            "Variant": variant,
            "Price": amount
        })
    return rows


# ----------------------------
# Combine everything into DataFrame
# ----------------------------
def fetch_toyota_prices(dealer_id=704):
    all_data = []
    models = fetch_toyota_models()
    for m in models:
        model_id, model_name = m["id"], m["name"]
        try:
            prices = _toyota_prices(dealer_id, model_id, model_name)
            all_data.extend(prices)
        except Exception as e:
            print(f"❌ Failed for {model_name}: {e}")
    return all_data


# =====================
# KIA SCRAPER
# =====================

KIA_API = "https://www.kia.com/api/kia2_in"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

# ----------------------------
# Clean Variant Name
# ----------------------------
def clean_variant(name: str) -> str:
    name = re.sub(r"^Kia\s+\w+\s+", "", name, flags=re.I)
    name = re.sub(r"\b(?:Smartstream|CRDI VGT?|T-?GDI|[DG]\d\.\d\w*|\d+\s?(?:MT|AT|DCT|iMT|IVT))\b", "", name, flags=re.I)
    name = re.sub(r"\s*-\s*", " ", name)
    return name.split("|")[0].strip()

# ----------------------------
# Normalize Transmission
# ----------------------------
def normalize_trans(name: str) -> str:
    name = name.upper()
    if "IMT" in name:
        return "iMT"
    if "MT" in name:
        return "MT"
    if any(x in name for x in ["AT", "DCT", "IVT"]):
        return "AT"
    return name or "NA"

# ----------------------------
# Fetch Models
# ----------------------------
def fetch_models(state="DL", city="N10"):
    url = f"{KIA_API}/configure.getModelList.do"
    resp = requests.post(url, headers=HEADERS, data={"stateCode": state, "cityCode": city})
    return [{"name": m["modelName"], "code": m["modelCode"]} for m in resp.json().get("data", [])]

# ----------------------------
# Fetch Variants for One Model
# ----------------------------
def fetch_variants(model, state="DL", city="N10"):
    url = f"{KIA_API}/configure.getVrntList.do"
    resp = requests.get(f"{url}?modelCode={model['code']}&stateCode={state}&cityCode={city}", headers=HEADERS)
    data = resp.json().get("data", {})

    engines = {e["dmsEngineCode"]: (e["engineName"], e["fuelType"]) for e in data.get("engines", [])}
    trans = {t["dmsTmdtCode"]: t["tmName"] for t in data.get("transmissions", [])}

    rows = []
    for v in data.get("variants", []):
        price = v.get("price", {}).get("M", {}).get("intraExsrPrice", 0)
        if price > 0:
            ocn = v.get("dmsMcOcn", "").split()
            code = ocn[-2] if len(ocn) >= 2 else ""
            engine_name, fuel = engines.get(code[4:-1], ("NA", "NA"))
            raw_trans = trans.get(code[-1:], "NA")
            transmission = normalize_trans(raw_trans)
            rows.append({
                "Brand": "Kia",
                "Model": model["name"],
                "Variant": clean_variant(v.get("variantName", "")),
                "Fuel": fuel,
                "Transmission": transmission,
                "Price": price
            })
    return rows

# ----------------------------
# Fast Parallel Fetch
# ----------------------------
def fetch_kia_prices(state="DL", city="N10", workers=8):
    models = fetch_models(state, city)
    all_data = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_variants, m, state, city): m for m in models}
        for fut in as_completed(futures):
            try:
                all_data.extend(fut.result())
            except Exception as e:
                print(f"❌ Failed for {futures[fut]['name']}: {e}")
    return all_data


# =====================
# MG SCRAPER
# =====================

MG_API = "https://eeysubngbk.execute-api.ap-south-1.amazonaws.com/prod/api/variants"
MG_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json",
    "Origin": "https://www.mgmotor.co.in",
    "Referer": "https://www.mgmotor.co.in/",
    "x-api-key": "xuQ7dH6jOq7L0ZmHVWkEw5HPYwXWYB2L8ASWJPn1",
}

def normalize_mg_trans(raw: str) -> str:
    if not raw:
        return "NA"
    raw = raw.upper()
    if "AUTM" in raw:
        return "AT"
    if "MANL" in raw:
        return "MT"
    return raw

# ----------------------------
# Map Fuel Type Codes
# ----------------------------
FUEL_MAP_MG = {
    "01": "Diesel",
    "02": "Petrol",
    "05": "EV"
}

def clean_mg_variant(variant: str) -> str:
    if not variant:
        return ""

    # Remove brand/model names (case-insensitive)
    name = re.sub(r"\b(?:MG|Astor|Hector|Gloster|Comet|ZS|Hectorplus6|Hectorplus7)\b", "", variant, flags=re.I)

    # Remove fuel types
    name = re.sub(r"\b(?:Petrol|Diesel|EV|Dsl|Hybrid)\b", "", name, flags=re.I)

    # Remove transmission types
    name = re.sub(r"\b(?:iMT|MT|AT|IVT|DCT|CVT|6MT)\b", "", name, flags=re.I)

    # Remove seat/number codes like 6S, 7S
    name = re.sub(r"\b\d+[A-Z]*\b", "", name, flags=re.I)

    # Clean separators and spaces
    name = re.sub(r"\s*-\s*", " ", name)
    name = re.sub(r"\s+", " ", name)

    return name.strip().title()



def normalize_mg_fuel(code: str) -> str:
    return FUEL_MAP_MG.get(code, "NA")

# ----------------------------
# Fetch MG Variants
# ----------------------------
def fetch_mg_prices(state="Delhi", city="Delhi"):
    resp = requests.get(MG_API, headers=MG_HEADERS)
    data = resp.json()
    rows = []

    for model in data:
        model_line = model.get("modelLine") or model.get("model_line")
        for v in model.get("variants", []):
            variant_name = clean_mg_variant(v.get("model_text1", ""))
            fuel = normalize_mg_fuel(v.get("fuel_type"))
            transmission = normalize_mg_trans(v.get("vehicle_type"))

            # find Delhi price only
            price = None
            for p in v.get("pricing", []):
                if p.get("State") == state:
                    for c in p.get("cities", []):
                        if c.get("City") == city and c.get("price"):
                            price = float(c["price"].strip())
                            break

            if price:
                rows.append({
                    "Brand": "MG",
                    "Model": model_line,
                    "Variant": variant_name,
                    "Fuel": fuel,
                    "Transmission": transmission,
                    "Price": price
                })
    return rows


# =====================
# Nissan SCRAPER
# =====================

BASE_URL = "https://www.nissan.in/prices-list.html"

headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/139.0.0.0 Safari/537.36"
}

# ----------------------------
# Helper: find model name for a table
# ----------------------------
def find_nissan_name_for_table(table):
    # 1) Prefer <h2 class="heading"> near the table
    prev = table.find_previous('h2', class_='heading')
    if prev and prev.get_text(strip=True):
        model = prev.get_text(" ", strip=True)
    else:
        # 2) Scan previous tags for the first one that mentions "Nissan"
        model = None
        for tag in table.find_all_previous():
            if tag.name in ('h2', 'h3', 'span', 'p', 'div') and tag.get_text(strip=True):
                text = tag.get_text(" ", strip=True)
                if re.search(r'\bNissan\b', text, re.I):
                    model = text
                    break
        # 3) fallback to nearest heading-like tag
        if not model:
            prev = table.find_previous(['h2', 'h3', 'p', 'strong'])
            model = prev.get_text(" ", strip=True) if prev and prev.get_text(strip=True) else "Unknown Model"

    # Normalize: remove leading "New " and "Nissan " from model name
    model = re.sub(r'^(New\s+)?Nissan\s+', '', model, flags=re.I).strip()
    return model if model else "Unknown Model"


# ----------------------------
# Function 1: Fetch all models -> returns dict { model_name: [table, ...] }
# ----------------------------
def fetch_nissan_models():
    resp = requests.get(BASE_URL, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")

    models = {}
    for table in soup.find_all("table"):
        model_name = find_nissan_name_for_table(table)
        models.setdefault(model_name, []).append(table)
    return models


# ----------------------------
# Parse fuel & transmission from variant string
# ----------------------------
def parse_fuel_trans(variant):
    fuel, trans = "", ""
    v = variant.upper()
    if re.search(r'\bCVT\b|\bAT\b|AUTOMATIC', v):
        trans = "Automatic"
    elif re.search(r'\bMT\b|\bMANUAL', v):
        trans = "Manual"
    elif re.search(r'\bEZ-SHIFT', v):
        trans = "AMT"

    if re.search(r'DIESEL', v):
        fuel = "Diesel"
    elif re.search(r'PETROL', v):
        fuel = "Petrol"
    return fuel, trans


# ----------------------------
# Clean variant name: remove "Nissan", "New Nissan", and transmission tokens
# ----------------------------
def clean_variant_name(variant, model_name):
    if model_name:
        variant = re.sub(re.escape(model_name), '', variant, flags=re.I)
    variant = re.sub(r'^(New\s+)?Nissan\s+', '', variant, flags=re.I)
    variant = re.sub(r'\b(MT|CVT|AT|Manual|Automatic|EZ-SHIFT|X-TRONIC)\b', '', variant, flags=re.I)
    variant = re.sub(r'\s{2,}', ' ', variant).strip()
    variant = variant.strip(" -–—:;()[]")

    return variant



# ----------------------------
# Function 2: Fetch prices for one model (accepts list of tables for that model)
# ----------------------------
def _nissan_prices(model_name, tables):
    rows = []
    for table in tables:
        for row in table.find_all("tr")[1:]:  # skip header row
            cols = [c.get_text(strip=True).replace("\xa0", " ") for c in row.find_all("td")]
            if len(cols) == 2:
                variant_raw, price_raw = cols
                # parse price
                clean_price = price_raw.replace(",", "").replace("₹", "").strip()
                try:
                    clean_price = int(clean_price)
                except:
                    clean_price = None

                fuel, trans = parse_fuel_trans(variant_raw)
                variant = clean_variant_name(variant_raw,model_name)

                rows.append({
                    "Brand": "Nissan",
                    "Model": model_name,          # normalized model (no 'Nissan' prefix)
                    "Fuel": "Petrol",
                    "Transmission": trans,
                    "Variant": variant,
                    "Price": clean_price
                })
    return rows


# ----------------------------
# Function 3: Combine everything into a DataFrame
# ----------------------------
def fetch_nissan_prices():
    all_data = []
    models = fetch_nissan_models()  # dict: model -> [table, ...]
    for model_name, tables in models.items():
        try:
            prices = _nissan_prices(model_name, tables)
            all_data.extend(prices)
        except Exception as e:
            print(f"❌ Failed for {model_name}: {e}")
    return all_data

# =====================
# MASTER SCRAPER (button triggers calls)
# =====================
def scrape_all_brands_parallel():
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_maruti = ex.submit(fetch_maruti_prices_parallel)
        f_tata = ex.submit(fetch_tata_prices_parallel)
        f_hyundai = ex.submit(fetch_hyundai_prices_parallel)
        f_mahindra = ex.submit(fetch_mahindra_prices_parallel)
        f_toyota = ex.submit(fetch_toyota_prices)
        f_kia =ex.submit(fetch_kia_prices)
        f_mg = ex.submit(fetch_mg_prices)
        f_nissan = ex.submit(fetch_nissan_prices)
        maruti = f_maruti.result()
        tata = f_tata.result()
        hyundai = f_hyundai.result()
        mahindra = f_mahindra.result()
        toyota = f_toyota.result()
        kia = f_kia.result()
        mg = f_mg.result()
        nissan = f_nissan.result()

    all_prices = (maruti or []) + (tata or []) + (hyundai or []) + (mahindra or []) + (toyota or []) + (kia or [])+ (mg or []) + (nissan or [])
    all_prices= remove_duplicates(all_prices)
    return all_prices
