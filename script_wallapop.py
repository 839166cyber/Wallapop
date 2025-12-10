import requests
import json
from datetime import datetime

# --- Configuración Específica para Motorbikes (Ajustado) ---

# URL del endpoint de búsqueda
URL = "https://api.wallapop.com/api/v3/search"

# Headers requeridos (Sección 4.2)
HEADERS = {
    "Host": "api.wallapop.com",
    "X-DeviceOS": "0"
}

# Parámetros de la consulta
PARAMS = {
    "source": "search_box",
    "keywords": "moto",        # Palabra clave 'moto'
    "category_id": "14000",    # ID para "Motorbike" 
    "latitude": "40.4129297",
    "longitude": "-3.695283",
    "time_filter": "today",    # OBLIGATORIO: Solo artículos publicados hoy [cite: 479]
    "order_by": "newest",
    "distance_in_km": "50",
}

# Nombre base para el archivo JSON de salida (Ajustado)
CATEGORY_TAG = "motorbikes"


def fetch_today_items():
    """
    Realiza una solicitud GET a la API de búsqueda de Wallapop.
    Retorna la lista de ítems encontrados para hoy.
    """
    print(f"Buscando ítems en la categoría '{CATEGORY_TAG}' (ID: {PARAMS['category_id']}) con la palabra clave '{PARAMS['keywords']}'.")
    items = []
    
    try:
        # Petición HTTP
        response = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=10)
        
        # Lanza una excepción si el código de estado es de error
        response.raise_for_status()
        
        # Parsear la respuesta JSON
        data = response.json()
        
        # Extraer la lista de ítems (puede necesitar ajuste si la ruta cambia) [cite: 618]
        items = data.get("data", {}).get("section", {}).get("payload", {}).get("items", [])
        
        print(f"✅ Búsqueda exitosa. Se han encontrado {len(items)} ítems.")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al conectar con la API de Wallapop: {e}")
    except json.JSONDecodeError:
        print(f"❌ Error al decodificar la respuesta JSON.")
    
    return items

# ----------------------------------------------------------------------

def save_items_to_daily_file(items):
    """
    Guarda la lista de ítems en un archivo JSON Lines diario con el nombre 'wallapop_motorbikes_YYYYMMDD.json'.
    """
    # Genera el sufijo de la fecha actual (YYYYMMDD)
    today = datetime.utcnow().strftime("%Y%m%d")
    
    # Nombre del archivo: wallapop_<category_or_tag>_<YYYYMMDD>.json [cite: 560]
    filename = f"wallapop_{CATEGORY_TAG}_{today}.json"
    
    # Abrir el archivo en modo escritura y guardar cada objeto JSON en una línea (JSON Lines) [cite: 565]
    with open(filename, "w", encoding="utf-8") as f:
        count = 0
        for item in items:
            # Escribe un objeto JSON por línea [cite: 565]
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
    
    print(f"💾 Guardados {count} ítems en el archivo: **{filename}**")
    print("El archivo está en formato JSON Lines, ideal para ingesta en Elastic.")


# ----------------------------------------------------------------------

if __name__ == "__main__":
    
    # 1. Adquisición de datos
    items = fetch_today_items()
    
    # 2. Guardar a archivo diario
    if items:
        save_items_to_daily_file(items)
    else:
        print("⚠️ No se encontraron ítems o hubo un error, no se generó el archivo diario.")