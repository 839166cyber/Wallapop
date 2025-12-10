import requests
import json
from datetime import datetime, timezone
import time
from statistics import mean, median, stdev
import os
import sys 

if os.name == "nt":  # Solo Windows
    import ctypes
    # Habilita UTF-8 en Windows CMD
    os.system("chcp 65001 >nul")
    sys.stdout.reconfigure(encoding='utf-8')
    
# --- CONFIGURACIÓN ESTÁTICA ---
URL = "https://api.wallapop.com/api/v3/search"
HEADERS = {
    "Host": "api.wallapop.com",
    "X-DeviceOS": "0"
}

# Coordenadas de Zaragoza (usadas en el poller y en la detección de riesgo genérico)
ZARAGOZA_LAT = "41.648823"
ZARAGOZA_LON = "-0.889085"


# --- FUNCIONES DE ADQUISICIÓN Y PERSISTENCIA (SIN CAMBIOS) ---

def get_daily_filename():
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"wallapop_motos_{today}.json"


def load_existing_ids(filename):
    existing_ids = set()
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        item = json.loads(line)
                        if item_id := item.get("id"):
                            existing_ids.add(item_id)
        except Exception:
            pass 
    return existing_ids


def fetch_all_pages(keywords, category_id, latitude=ZARAGOZA_LAT, longitude=ZARAGOZA_LON):
    all_items = []
    offset = 0
    limit = 50
    page = 1
    
    while True: 
        params = {
            "source": "search_box",
            "keywords": keywords,
            "category_id": str(category_id),
            "latitude": latitude,
            "longitude": longitude,
            "time_filter": "today",
            "order_by": "newest",
            "offset": offset,
            "limit": limit
        }
        
        try:
            response = requests.get(URL, params=params, headers=HEADERS, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("data", {}).get("section", {}).get("payload", {}).get("items", [])
            
            if not items:
                break
            
            all_items.extend(items)
            
            if len(items) < limit:
                break
            
            offset += limit
            page += 1
            time.sleep(0.5)
        
        except Exception:
            break
    
    return all_items

def remove_duplicates(items):
    seen_ids = set()
    unique_items = []
    duplicates_removed = 0
    
    for item in items:
        item_id = item.get("id")
        if item_id and item_id not in seen_ids:
            seen_ids.add(item_id)
            unique_items.append(item)
        else:
            duplicates_removed += 1
    
    return unique_items, duplicates_removed

def save_daily_file(all_items, filename):
    with open(filename, "a", encoding="utf-8") as f:
        for item in all_items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


# --- FUNCIONES DE FILTRADO Y ENRIQUECIMIENTO ---

def is_clothing_or_personal_gear(item):
    title = item.get("title", "").lower()
    description = item.get("description", "").lower()
    
    CLOTHING_KEYWORDS = [
        "casco", "guante", "chaqueta", "pantalón", "pantalon", "botas", 
        "espaliers", "espalda", "goretex", "chamarra", "bota", "mono", 
        "traje", "talla", "alforja", "mochila","maleta", "chaleco",
        "protector", "protección", "impermeable", "capa de lluvia ", "zapatos", 
        "caballete", "mini", "herramientas", "candado", "antirrobo", "cubremanos",
        "alquiler", "garaje", "baul", "maleta"
    ] 
    
    if any(keyword in title for keyword in CLOTHING_KEYWORDS):
            return True 
    if any(keyword in description for keyword in CLOTHING_KEYWORDS):
            return True 

    return False

def filter_clothing_items(items):
    filtered_items = []
    removed_count = 0
    for item in items:
        if not is_clothing_or_personal_gear(item):
            filtered_items.append(item)
        else:
            removed_count += 1
            
    return filtered_items, removed_count


def detect_suspicious_keywords(text):
    """
    Busca todas las palabras sospechosas y las clasifica.
    Retorna la lista de palabras encontradas y un set con las categorías de riesgo activadas.
    """
    
    # DICCIONARIO ÚNICO DE RIESGOS (incluye estructural y general para simplificar)
    # Formato: {categoria: [lista de palabras clave]}
    RISK_CATEGORIES = {
        "CRITICAL_LEGAL": ["sin papeles", "sin documentacion", "sin documento", "no papeles", "papeles pendientes", "transferencia pendiente"],
        "CRITICAL_INTEGRITY": ["sin itv", "sin itp", "no itv", "no itp", "itv caducada", "itp caducada", 
                               "para piezas", "para despiece", "despiece", "solo piezas", "km desconocidos", "kilometraje desconocido"],
        "CRITICAL_FRAUD": ["robo_potencial", "importacion", "importada", "procedencia dudosa", "comprada en", "encontrada"],
        "GENERAL_URGENCY": ["urgente", "solo hoy", "solo esta semana", "rapido", "rápido", "venta rapida"],
        "GENERAL_PRICE_BASED": ["ganga", "precio bajo", "muy barato", "chollo", "oferta"],
    }
    
    if not text:
        return [], set()
    
    text_lower = text.lower()
    found_keywords = []
    found_categories = set()
    
    for category, keywords in RISK_CATEGORIES.items():
        for keyword in keywords:
            if keyword in text_lower:
                found_keywords.append(keyword)
                found_categories.add(category) # Registra qué categoría de riesgo se activó
    
    return found_keywords, found_categories


def calculate_relative_price_index(price, all_prices):
    if not all_prices or not price:
        return 1.0
    
    avg_price = mean(all_prices)
    if avg_price == 0:
        return 1.0
    
    return round(price / avg_price, 2)

def calculate_risk_score(item, all_prices, seller_items_count, found_categories, text_lower):
    """Calcula risk_score usando las categorías de riesgo encontradas."""
    score = 0
    
    # --- 1. Ponderación de Palabras Clave (Usando el resultado de la búsqueda) ---
    
    if "CRITICAL_LEGAL" in found_categories or "CRITICAL_INTEGRITY" in found_categories or "CRITICAL_FRAUD" in found_categories:
        score += 30 # Riesgo Crítico (Legal, estructural, o robo)
        
    if "GENERAL_URGENCY" in found_categories or "GENERAL_PRICE_BASED" in found_categories:
        score += 15 # Riesgo General (Urgencia/Precio)
    
    # --- 2. Factores de Precio, Condición y Geolocalización ---
    
    if all_prices:
        avg_price = mean(all_prices)
        price = item.get("price", {}).get("amount")
        
        if price:
            # Factor: Precio muy bajo (Base)
            if price < avg_price * 0.4:
                score += 40
            elif price < avg_price * 0.6:
                score += 20
                
            # Factor: Inconsistencia Precio / Condición 
            CONDITION_KEYWORDS = ["como nueva", "perfecto estado", "muy buen estado", "impecable"]
            if price < avg_price * 0.7: # Precio sospechosamente bajo
                 if any(kw in text_lower for kw in CONDITION_KEYWORDS):
                     score += 20 # 20 puntos por alto riesgo de inconsistencia

    # Factor: Descripción muy corta
    description = item.get("description", "")
    if description and len(description) < 50:
        score += 10
    
    # Factor: Muchos anuncios del mismo vendedor
    if seller_items_count and seller_items_count > 3:
        score += 20
    
    # Factor: Sin imágenes 
    images = item.get("images", [])
    if not images:
        score += 5
        
    # Factor: Riesgo de Geolocalización Genérica
    location = item.get("location", {})
    if str(location.get("latitude")) == ZARAGOZA_LAT and \
       str(location.get("longitude")) == ZARAGOZA_LON:
        score += 10 # Penaliza si usa las coordenadas de búsqueda por defecto
    
    
    return min(score, 100)

def enrich_items(items):
    """Enriquece todos los items con campos calculados."""
    prices = [item.get("price", {}).get("amount") for item in items 
              if item.get("price", {}).get("amount")]
    prices = [p for p in prices if p is not None and p > 0]
    
    seller_counts = {}
    for item in items:
        seller_id = item.get("user_id")
        if seller_id:
            seller_counts[seller_id] = seller_counts.get(seller_id, 0) + 1
    
    enriched_items = []
    
    for item in items:
        enriched = item.copy()
        
        enriched["crawl_timestamp"] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        price = item.get("price", {}).get("amount")
        enriched["relative_price_index"] = calculate_relative_price_index(price, prices)
        
        text = f"{item.get('title', '')} {item.get('description', '')}"
        
        # --- LLAMADA ÚNICA PARA OBTENER PALABRAS Y CATEGORÍAS ---
        found_keywords, found_categories = detect_suspicious_keywords(text)
        
        seller_id = item.get("user_id")
        seller_count = seller_counts.get(seller_id, 0)
        
        # Cálculo del score usando las categorías encontradas
        risk_score = calculate_risk_score(item, prices, seller_count, found_categories, text.lower())
        
        # Creación del diccionario de enriquecimiento (Sección 3.3)
        enriched["enrichment"] = {
            "suspicious_keywords": list(set(found_keywords)), 
            "suspicious_keywords_count": len(set(found_keywords)),
            "risk_score": risk_score,
            "relative_price_index": enriched["relative_price_index"],
            "seller_items_today": seller_count,
            "description_length": len(item.get("description", "")),
            "has_images": len(item.get("images", [])) > 0,
            "image_count": len(item.get("images", []))
        }
        
        enriched_items.append(enriched)
    
    return enriched_items


def print_statistics(items):
    print("\n" + "=" * 70)
    print("📊 ESTADÍSTICAS DEL DATASET")
    print("=" * 70)
    
    prices = [item.get("price", {}).get("amount") for item in items 
              if item.get("price", {}).get("amount")]
    prices = [p for p in prices if p and p > 0]
    
    risk_scores = [item.get("enrichment", {}).get("risk_score", 0) for item in items]
    
    if prices:
        print(f"💰 Precios:")
        print(f"   Min: {min(prices):.2f}€ | Max: {max(prices):.2f}€ | Media: {mean(prices):.2f}€")
        if len(prices) > 1:
            print(f"   Mediana: {median(prices):.2f}€ | Desv. Std: {stdev(prices):.2f}€")
    
    if risk_scores:
        print(f"\n⚠️  Risk Scores:")
        print(f"   Min: {min(risk_scores)} | Max: {max(risk_scores)} | Media: {mean(risk_scores):.1f}")
        
        high_risk = len([r for r in risk_scores if r >= 70])
        medium_risk = len([r for r in risk_scores if 40 <= r < 70])
        low_risk = len([r for r in risk_scores if r < 40])
        
        print(f"\n   🔴 ALTO RIESGO (>=70): {high_risk}")
        print(f"   🟡 RIESGO MEDIO (40-69): {medium_risk}")
        print(f"   🟢 BAJO RIESGO (<40): {low_risk}")
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    
    print("=" * 70)
    print("Obteniendo MOTOS, PIEZAS Y ACCESORIOS (EXCLUYENDO INDUMENTARIA) de hoy")
    print("=" * 70 + "\n")
    
    # --- 1. Deduplicación Persistente y Carga ---
    daily_filename = get_daily_filename()
    existing_ids = load_existing_ids(daily_filename)
    print(f"Cargados {len(existing_ids)} IDs existentes para el día.")
    
    all_items = []
    
    # 2. Búsqueda (Adquisición de datos)
    search_queries = [
        ("moto", 14000), # Categoría Motorbike
    ]
    
    for keywords, category_id in search_queries:
        print(f"🔍 Buscando: '{keywords}' (category_id={category_id})")
        items = fetch_all_pages(keywords, category_id) 
        
        if items:
            print(f"   → Total para '{keywords}' adquirido: {len(items)} items\n")
            all_items.extend(items)
        else:
            print(f"   ℹ No se encontraron items\n")
    
    # 3. Limpieza de Datos (Deduplicación Interna)
    print(f"Eliminando duplicados internos de {len(all_items)} items...")
    unique_items, dupes_internal = remove_duplicates(all_items)
    print(f"✓ Items únicos (internos): {len(unique_items)} | Duplicados eliminados: {dupes_internal}\n")
    
    # 4. Filtrado de Ruido (Indumentaria)
    print("Filtro de ruido: eliminando indumentaria y equipo personal...")
    filtered_items, removed_clothing_count = filter_clothing_items(unique_items)
    print(f"✓ Ítems descartados (Indumentaria): {removed_clothing_count}")
    print(f"✓ Ítems para análisis: {len(filtered_items)}\n")
    
    # 5. Filtrado de Duplicados Persistentes (Ítems ya guardados hoy)
    new_items_to_save = [
        item for item in filtered_items if item.get("id") not in existing_ids
    ]
    duplicates_external = len(filtered_items) - len(new_items_to_save)
    print(f"✓ Ítems descartados (ya estaban en JSON): {duplicates_external}")
    print(f"✓ Ítems NUEVOS listos para enriquecer y guardar: {len(new_items_to_save)}\n")
    
    # 6. Enriquecimiento (SOLO los nuevos)
    print("Enriqueciendo SOLO datos nuevos...")
    enriched_new_items = enrich_items(new_items_to_save)
    print(f"✓ {len(enriched_new_items)} items enriquecidos\n")
    
    # 7. Estadísticas e Ingesta
    if enriched_new_items:
        print_statistics(enriched_new_items)
        save_daily_file(enriched_new_items, daily_filename)
        print(f"✅ Archivo guardado: {daily_filename}. Los datos se anexarán en futuras llamadas.")
    else:
        print("\n⚠️  No hay datos NUEVOS para guardar.")