# Notas de Migración: Auditoría de Seguridad & AuthService

Este documento contiene las observaciones de la auditoría inicial de seguridad realizada en la Fase 0 para preparar el sistema de autenticación de VibeCloud hacia tokens JWT.

## Estado de AuthService (`services/auth_service.py`)

* **Control de Contraseñas:** El servicio utiliza `passlib.context.CryptContext` con esquemas `argon2` y `bcrypt` de forma correcta.
* **Autenticación Actual:** La autenticación se realiza exclusivamente a través de cookies de sesión firmadas mediante `SessionMiddleware` de Starlette.
* **Soporte de JWT:**
  * **No existe soporte de JWT nativo en la base de código actual.**
  * No hay utilidades para la generación, firmado, decodificación o renovación de tokens JWT.
  * No existe una estructura de persistencia (tabla) para tokens de refresco (`refresh_token`).

## Requerimientos para la Fase 1

Para desacoplar el frontend (Next.js) y permitir consumo vía API REST, se implementarán los siguientes cambios en la Fase 1:
1. **Creación de `services/jwt_service.py`:** Utilizando la librería `python-jose` (o `pyjwt`) para manejar tokens de acceso (15 minutos) y de refresco (7 días).
2. **Tabla `RefreshToken`:** Modelo en SQLModel para persistir hashes de tokens de refresco activos por usuario y tenant.
3. **Controladores de Auth en `/api/v1/auth/`:** Endpoints para `/login`, `/refresh` y `/logout`.
4. **Dependencia `get_current_user_jwt`:** Nuevo inyector de dependencias para validar la cabecera `Authorization: Bearer <token>` de forma aislada a las dependencias de sesiones por cookies.
