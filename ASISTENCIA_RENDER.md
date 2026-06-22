# Guía de Configuración para Render + Supabase

Has creado el proyecto en Supabase: **`gfzhdtoytgoznrmysfan`**

Para que tu backend en Render funcione, necesitas configurar las siguientes **Environment Variables** (Variables de Entorno) en el Dashboard de Render:

## 1. SUPABASE_URL
*   **Valor:** `https://gfzhdtoytgoznrmysfan.supabase.co`

## 2. SUPABASE_KEY
Esta es tu llave pública para el cliente de Supabase.
1.  Ve a tu Dashboard de Supabase.
2.  Entra en **Project Settings** (engranaje abajo a la izquierda) -> **API**.
3.  Copia el valor de **`anon`** / `public`.

## 3. DATABASE_URL (¡Crucial!)
Esta es la cadena de conexión para que Python hable con PostgreSQL. 

> [!IMPORTANT]
> Supabase ha dejado de dar soporte IPv4 gratuito para la conexión directa (puerto 5432 de `db.gfzhdtoytgoznrmysfan.supabase.co`).
> Como Render no soporta salida IPv6 de forma predeterminada, la conexión directa fallará o se quedará colgada indefinidamente.
> **Debes usar la cadena de conexión del Connection Pooler (que soporta IPv4 de forma gratuita).**

1.  Ve a **Project Settings** -> **Database**.
2.  Busca la sección **Connection String**.
3.  Selecciona la pestaña **URI**.
4.  Copia la cadena de conexión del pooler. Se verá así:
    `postgresql://postgres.gfzhdtoytgoznrmysfan:[TU_PASSWORD]@aws-1-us-east-2.pooler.supabase.com:6543/postgres?sslmode=require`
5.  **Reemplaza `[TU_PASSWORD]`** con la contraseña que creaste para la base de datos de Supabase.
6.  Asegúrate de que el usuario de la cadena sea `postgres.gfzhdtoytgoznrmysfan` y no solo `postgres`.

---

## Confirmación en Render
Cuando Render te pida las variables, pega estos 3 valores.
*   Si falla la conexión a base de datos, verifica que la contraseña sea correcta.
*   Si dice "Missing dependencies", verifica que `requirements.txt` esté instalado (Render lo hace solo).

¡Tu sistema de facturación estará listo en la nube! 🚀

