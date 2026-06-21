import httpx
import json
import logging

logger = logging.getLogger(__name__)

class GeminiService:
    @staticmethod
    async def _call_gemini_api(prompt: str, system_instruction: str, api_key: str) -> str:
        """
        Calls Google Gemini 2.0 Flash API via native HTTP request using httpx.
        Forces JSON response format.
        """
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ],
            "systemInstruction": {
                "parts": [{"text": system_instruction}]
            },
            "generationConfig": {
                "responseMimeType": "application/json"
            }
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, timeout=30.0)
                if response.status_code != 200:
                    logger.error(f"Gemini API Error: Status {response.status_code}, Body: {response.text}")
                    raise ValueError(f"Error de Gemini API: {response.status_code}")
                
                res_data = response.json()
                # Parse content from response structure
                candidates = res_data.get("candidates", [])
                if not candidates:
                    raise ValueError("No candidates returned from Gemini API")
                
                parts = candidates[0].get("content", {}).get("parts", [])
                if not parts:
                    raise ValueError("No content parts returned from Gemini API")
                
                text_content = parts[0].get("text", "")
                return text_content
            except httpx.RequestError as e:
                logger.error(f"HTTP Request to Gemini failed: {e}")
                raise ValueError(f"Error de conexión con la API de Gemini: {e}")

    @classmethod
    async def generate_ui_design(cls, prompt: str, current_layout: dict, current_theme: dict, api_key: str) -> dict:
        """
        Generates layout and theme configurations based on user natural language input.
        """
        system_instruction = """
        Eres un experto diseñador de interfaces y arquitecto frontend de Server-Driven UI (SDUI).
        Tu trabajo es generar un diseño de UI (layout) y tema estético en formato JSON basado en el prompt del usuario y la configuración actual.

        Debes retornar EXCLUSIVAMENTE un objeto JSON con dos llaves principales: "layout" y "theme".

        El catálogo de componentes de presentación disponibles es:
        - "Header": Cabecera con título del POS, información del Tenant y usuario.
        - "ProductSearchBox": Buscador de productos por código de barras o nombre.
        - "CatalogGrid": Grilla de catálogo con paginación y tarjetas de productos.
        - "CartDetail": Detalle del carrito de compras con botones de incremento/decremento.
        - "PaymentSection": Métodos de cobro (efectivo, transferencia, mixto) y botón de cierre de venta.
        - "SalesHistory": Historial de ventas recientes.
        - "Footer": Barra de estado inferior.

        Esquema del JSON de salida esperado:
        {
          "layout": {
            "modules": ["Header", "ProductSearchBox", "CatalogGrid", "CartDetail", "PaymentSection", "Footer"],
            "grid_cols": 12,
            "layout_structure": {
              "Header": {"row": 1, "col_span": 12},
              "ProductSearchBox": {"row": 2, "col_span": 12},
              "CatalogGrid": {"row": 3, "col_span": 8},
              "CartDetail": {"row": 3, "col_span": 4},
              "PaymentSection": {"row": 4, "col_span": 12},
              "Footer": {"row": 5, "col_span": 12}
            }
          },
          "theme": {
            "primary_color": "#HEX",
            "secondary_color": "#HEX",
            "mode": "dark" o "light",
            "background_gradient": "degradado CSS (ej. linear-gradient(135deg, #1e1b4b, #311042))",
            "border_radius": "px o rem",
            "font_family": "fuente de Google Fonts (ej. 'Inter', 'Outfit')"
          }
        }
        """

        user_prompt = f"""
        Configuración actual:
        Layout actual: {json.dumps(current_layout)}
        Tema actual: {json.dumps(current_theme)}

        Prompt del usuario: "{prompt}"

        Genera una interfaz optimizada, estética y premium que se ajuste al prompt. Retorna solo el JSON estructurado.
        """

        result_text = await cls._call_gemini_api(user_prompt, system_instruction, api_key)
        try:
            return json.loads(result_text)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse Gemini output as JSON: {result_text}")
            raise ValueError("La respuesta de Gemini no es un JSON válido.")

    @classmethod
    async def generate_daily_theme(cls, date_str: str, api_key: str) -> dict:
        """
        Generates a theme configuration adapted to the date (seasons, events, holidays).
        """
        system_instruction = """
        Eres un diseñador de interfaces creativo y experto en branding y psicología del color.
        Tu trabajo es generar un tema visual estético (colores, gradientes, fuentes, bordes) adaptado a la festividad, estación del año o evento mundial especial de la fecha proporcionada.

        Debes retornar EXCLUSIVAMENTE un objeto JSON con una llave principal "theme":
        {
          "theme": {
            "primary_color": "#HEX",
            "secondary_color": "#HEX",
            "mode": "dark" o "light",
            "background_gradient": "degradado CSS (ej. linear-gradient(135deg, #1e1b4b, #311042))",
            "border_radius": "px o rem",
            "font_family": "fuente de Google Fonts"
          }
        }
        """

        prompt = f"""
        Fecha de hoy: {date_str}

        Analiza si hay eventos importantes (por ejemplo: Mundial de Fútbol, Navidad, Día del Padre, Halloween) o el cambio de estación (Verano, Primavera, Otoño, Invierno) asociado a esta época en general.
        Genera un tema estético premium (colores HSL/Hex, gradientes) que represente esta fecha o época.
        Por ejemplo, si es época del Mundial, usa tonos celestes/blancos o deportivos; si es Navidad, tonos rojos/verdes o invernales dorados; si es verano, colores cálidos y frescos.
        Retorna solo el JSON estructurado.
        """

        result_text = await cls._call_gemini_api(prompt, system_instruction, api_key)
        try:
            return json.loads(result_text)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse Gemini daily theme output as JSON: {result_text}")
            raise ValueError("La respuesta de Gemini para el tema diario no es un JSON válido.")
