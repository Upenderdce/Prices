import requests
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import re

# =====================
# CONFIG
# =====================
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

FUEL_MAP = {"1-D1MGNW9": "CNG", "1-ID-1738": "Diesel", "1-ID-267": "Petrol", "1-ID-268": "CNG"}
TRANS_MAP = {"5-251EY13B": "MT", "5-251EY13H": "AMT", "5-251EY13J": "AT","DCA":"AT", "DCT":"AT"}

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
        variant_name = (
            v.get("variantLabel", "")
            .replace(model_cfg["name"], "")  # remove model name (Curvv, Nexon, etc.)
            .replace(model_cfg["name"].upper(), "")  # remove uppercase model name
            .replace("-", " ")  # replace hyphens with spaces
            .strip()
        )

        # Remove fuel info
        for fuel_word in ["Petrol/Ethanol", "Petrol", "Diesel","PETROL", "DIESEL" ]:
            variant_name = variant_name.replace(fuel_word, "")

        # Remove "Standard"
        variant_name = variant_name.replace("Standard", "")
        # Remove MT / 5MT
        variant_name = re.sub(r"\b(5MT|MT|New)\b", "", variant_name)

        # Remove Bi-fuel tags
        variant_name = re.sub(r"\b(Bi[- ]?fuel.*|BIFUEL.*)\b", "", variant_name, flags=re.IGNORECASE)

        # Keep only ONE CNG
        variant_name = re.sub(r"(,\s*)?CNG\b", " CNG", variant_name, flags=re.IGNORECASE)

        # Remove duplicate CNG words if repeated
        variant_name = re.sub(r"\b(CNG)(\s*CNG)+\b", r"\1", variant_name, flags=re.IGNORECASE)

        # Cleanup commas and spaces
        variant_name = re.sub(r"\s+,", ",", variant_name)  # tidy commas
        variant_name = re.sub(r"\s{2,}", " ", variant_name)  # collapse spaces
        variant_name = variant_name.strip(" ,")

        out.append({
            "Brand": "Tata",
            "Model": model_cfg["name"],
            "Fuel": FUEL_MAP.get(fuel, fuel),
            "Transmission": TRANS_MAP.get(trans, trans),
            "Variant": variant_name,
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
                    "Variant": re.sub(r"\b(5MT|MT)\b", "",
                                      v.get("variantName", "").replace(modelName, "").replace("AGS", "AMT")).strip(),
                    "Price": price
                })
    except:
        pass
    print(rows)
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
                    "Variant": re.sub(r"\b(5MT|MT)\b", "",
                                      variant.get("variantName", "").replace(modelName, "").replace("AGS", "AMT")).strip(),
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
            print(v)
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
        print(soup.prettify())
        input_tag = soup.find("input", {"class": "js-radio"})
        variant_name = (
            input_tag.attrs.get("data-variantName") or
            input_tag.attrs.get("data-variantname") or
            "N/A"
        )
        price_tag = soup.find("span", {"class": "approx-price"})
        price_text = price_tag.text.strip() if price_tag else "N/A"
        price_int = _parse_price_rupees(price_text)
        variant_name = variant_name.strip()
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
            "Fuel": fuel,
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

KIA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
}

KIA_HOME = "https://www.kia.com/in/home.html"
KIA_URL = "https://www.kia.com"


# ----------------------------
# Function 1: Fetch all Kia models
# ----------------------------
def fetch_kia_models():
    """Fetch all Kia models (name + URL)."""
    resp = requests.get(KIA_HOME, headers=KIA_HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")

    models = []
    for li in soup.select(".gnb-item.tier-vehicles.has-d2 ul.d2-list > li.d2"):
        a_tag = li.select_one("a.d2-a > span.text")
        href_tag = li.select_one("a.d2-a")
        if a_tag and a_tag.get_text(strip=True) and href_tag:
            models.append({
                "name": a_tag.get_text(strip=True),
                "url": KIA_URL + href_tag['href']
            })
    return models


# ----------------------------
# Function 2: Fetch variants & prices for a model
# ----------------------------
def _kia_prices(model_name, model_url):
    """Fetch variant, fuel, transmission, and price for a Kia model."""
    showroom_url = model_url.replace(".html", "/showroom.html")
    resp = requests.get(showroom_url, headers=KIA_HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = []
    for trim_card in soup.select("section.trim-card"):
        # Variant name
        variant_tag = trim_card.select_one("h3.h4")
        variant = variant_tag.get_text(strip=True) if variant_tag else ""

        # Fuel and transmission info (if present)
        details_tag = trim_card.select_one("ul.spec-list li")
        fuel, trans = "", ""
        if details_tag:
            details_text = details_tag.get_text(strip=True)
            # Simple heuristic: split by '/' or known keywords
            parts = details_text.split('/')
            if len(parts) >= 2:
                fuel = parts[0].strip()
                trans = parts[1].strip()
            elif len(parts) == 1:
                fuel = parts[0].strip()

        # Price
        price_tag = trim_card.select_one("span.price script")
        price = None
        if price_tag and price_tag.string:
            match = re.search(r"ComUtils\.currency\((\d+)\)", price_tag.string)
            if match:
                price = int(match.group(1))

        rows.append({
            "Brand": "Kia",
            "Model": model_name,
            "Variant": variant,
            "Fuel": fuel,
            "Transmission": trans,
            "Price": price
        })
    return rows


# ----------------------------
# Function 3: Combine everything
# ----------------------------
def fetch_kia_prices():
    all_data = []
    models = fetch_kia_models()
    for m in models:
        model_name, model_url = m["name"], m["url"]
        try:
            prices = _kia_prices(model_name, model_url)
            all_data.extend(prices)
        except Exception as e:
            print(f"❌ Failed for {model_name}: {e}")
    return all_data


# =====================
# MG SCRAPER
# =====================
MG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MG_URL = "https://www.mgmotor.co.in"


# ----------------------------
# Function 1: Fetch all MG models
# ----------------------------
def fetch_mg_models():
    """Fetch all MG models (name + URL)."""
    resp = requests.get(MG_URL, headers=MG_HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")

    models = []
    for li in soup.select("ul#vechicles li a"):
        model_name = li.get_text(strip=True)
        model_url = MG_URL + li['href']
        models.append({
            "name": model_name,
            "url": model_url
        })
    return models


# ----------------------------
# Function 2: Fetch variants & prices for a model
# ----------------------------
def _mg_prices(model_name, model_url):
    """Fetch variant, fuel, transmission, and price for an MG model."""
    resp = requests.get(model_url, headers=MG_HEADERS)
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = []
    for card in soup.select("div.item[data-attribute='model-variant']"):
        # Variant name
        variant_tag = card.select_one("p.card-text")
        variant = variant_tag.get_text(strip=True) if variant_tag else ""

        # Price
        price_tag = card.select_one("p.model-price")
        price = None
        if price_tag:
            # Extract numeric value, remove currency symbols and commas
            price_text = price_tag.get_text(strip=True)
            price_match = re.search(r"(\d[\d,]*)", price_text)
            if price_match:
                price = int(price_match.group(1).replace(",", ""))

        # Fuel & Transmission info: MG site usually doesn't have them; leave empty
        fuel, trans = "", ""

        rows.append({
            "Brand": "MG Motors",
            "Model": model_name,
            "Variant": variant,
            "Fuel": fuel,
            "Transmission": trans,
            "Price": price
        })
    return rows


# ----------------------------
# Function 3: Combine everything
# ----------------------------
def fetch_mg_prices():
    all_data = []
    models = fetch_mg_models()
    for m in models:
        model_name, model_url = m["name"], m["url"]
        try:
            prices = _mg_prices(model_name, model_url)
            all_data.extend(prices)
        except Exception as e:
            print(f"❌ Failed for {model_name}: {e}")
    return all_data


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

    if re.search(r'TURBO', v):
        fuel = "Petrol Turbo"
    elif re.search(r'DIESEL', v):
        fuel = "Diesel"
    elif re.search(r'PETROL', v):
        fuel = "Petrol"
    return fuel, trans


# ----------------------------
# Clean variant name: remove "Nissan", "New Nissan", and transmission tokens
# ----------------------------
def clean_variant_name(variant):
    # remove Nissan prefix
    variant = re.sub(r'^(New\s+)?Nissan\s+', '', variant, flags=re.I)
    # remove transmission tokens and extra parentheses/brackets remnants
    variant = re.sub(r'\b(MT|CVT|AT|Manual|Automatic)\b', '', variant, flags=re.I)
    # collapse multiple spaces and strip
    variant = re.sub(r'\s{2,}', ' ', variant).strip()
    # remove leading/trailing punctuation
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
                variant = clean_variant_name(variant_raw)

                rows.append({
                    "Brand": "Nissan",
                    "Model": model_name,          # normalized model (no 'Nissan' prefix)
                    "Fuel": fuel,
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
    return all_prices