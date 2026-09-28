# Tarea 3 - Beam avanzado

Trabajo de la materia **Streaming de datos y sus aplicaciones**. El objetivo es calcular el total de pagos `CONFIRMED` por comercio y minuto, considerando eventos fuera de orden, duplicados, datos tardíos y reintentos al escribir el resultado.

## Configuración

- Se usa `event_time` como tiempo del evento.
- Las ventanas son fijas de 60 segundos.
- Se permiten hasta 120 segundos de lateness.
- El trigger usa `AfterWatermark`, con una emisión early a los 10 segundos y emisiones late cuando llegan nuevos eventos tardíos.
- Los panes son acumulativos.
- La deduplicación guarda los `event_id` vistos por comercio y ventana.
- Un timer elimina ese estado al terminar la ventana más la lateness permitida.
- El sink usa una clave `merchant_id|window_start` para hacer UPSERT y evitar duplicados por reintentos.

## Decisiones principales

### Tiempo de evento y ventanas

Los pagos se agrupan por el momento en que ocurrieron y no por el momento en que llegaron al sistema. De esta forma, un pago retrasado sigue quedando en el minuto que le corresponde.

### Datos tardíos

La lateness permitida es de 120 segundos. Si un evento llega dentro de ese margen, todavía puede corregir el total de su ventana. Si llega después, se considera demasiado tardío.

Aumentar este tiempo permite aceptar más eventos atrasados, pero también obliga a mantener el estado por más tiempo.

### Deduplicación y limpieza del estado

Se guarda cada `event_id` ya procesado para no contar dos veces el mismo pago. El estado se maneja por comercio y ventana.

También se usa un timer para limpiar los IDs cuando la ventana ya no necesita recibir correcciones. Esto evita que el estado crezca sin límite.

### Reintentos del sink

Una escritura puede completarse y aun así repetirse si el worker falla antes de confirmar el progreso. Para evitar filas duplicadas, se usa UPSERT con la clave:

```text
merchant_id|window_start
```

Si se usara un sink append-only, cada reintento agregaría otra fila.

## Dataset base

Con `data/payments.jsonl` y la configuración por defecto se espera:

- 9 eventos de entrada;
- 5 eventos aceptados;
- 1 duplicado;
- 1 evento `CONFIRMED` fuera de lateness (`p-007`);
- 2 eventos no confirmados (`PENDING` / `REJECTED`);
- 4 resultados finales.

| Comercio | Ventana | Total |
|---|---|---:|
| `m-azul` | 13:00-13:01 | 170000 |
| `m-verde` | 13:00-13:01 | 80000 |
| `m-verde` | 13:01-13:02 | 90000 |
| `m-azul` | 13:02-13:03 | 200000 |

Con 180 segundos de lateness, `p-007` también se acepta y el total de `m-verde` para 13:00-13:01 pasa a 110000.

## Ejecutar con uv

Requiere Python 3.12 y `uv`.

```bash
uv sync --frozen
uv run marimo edit notebook.py
```

Para correr las pruebas y validaciones:

```bash
uv run pytest
uv run ruff check notebook.py
uv run marimo check --strict notebook.py
```

## Ejecutar con Docker

```bash
docker compose up --build notebook
```

Luego abrir:

```text
http://localhost:2718
```

Para ejecutar las pruebas dentro del contenedor:

```bash
docker compose exec notebook uv run pytest
```

## Entrega

Antes de entregar, conviene ejecutar todas las pruebas y verificar que la suite quede verde. La entrega es solamente el enlace público al repositorio de GitHub.
