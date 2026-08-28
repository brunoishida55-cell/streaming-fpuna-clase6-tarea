# Tarea 3 — Beam avanzado

Implementación de la Tarea 3 de la asignatura **Streaming de datos y sus aplicaciones**.

El proyecto implementa un pipeline de pagos con Apache Beam capaz de trabajar con tiempo de evento, ventanas, eventos tardíos, deduplicación con estado, timers, triggers y una salida tolerante a reintentos mediante idempotencia.

## Objetivo

Producir los totales de pagos `CONFIRMED` por comercio y por minuto, aun cuando los eventos:

- lleguen fuera de orden;
- lleguen con atraso;
- estén duplicados;
- o la escritura del resultado sea reintentada.

## Contrato temporal

La implementación utiliza las siguientes reglas:

- timestamp del dominio: `event_time`;
- ventanas fijas de 60 segundos;
- intervalos de ventana de la forma `[window_start, window_end)`;
- lateness permitida: 120 segundos;
- solamente los eventos con `status == "CONFIRMED"` modifican los totales;
- un evento tardío dentro de la tolerancia puede corregir un resultado previo;
- un evento que supera la tolerancia queda auditado como `too_late`.

El uso de `event_time` permite que un evento fuera de orden sea asignado a la ventana donde ocurrió realmente, independientemente de su `arrival_time`.

## Contrato determinista

Antes del pipeline Beam se implementó `summarize_payments`, utilizado como oráculo determinista.

Para cada evento se conserva una auditoría con:

- `event_id`;
- `merchant_id`;
- `delay_seconds`;
- `duplicate`;
- `too_late`;
- `accepted`;
- `revision`;
- `reason`.

Con el dataset provisto y la configuración por defecto se procesan 9 eventos, se aceptan 5 y se producen 4 totales:

| merchant_id | ventana | total |
|---|---|---:|
| `m-azul` | 13:00–13:01 | 170000 |
| `m-azul` | 13:02–13:03 | 200000 |
| `m-verde` | 13:00–13:01 | 80000 |
| `m-verde` | 13:01–13:02 | 90000 |

El evento `p-007` presenta 169 segundos de atraso. Con `allowed_lateness=120` queda fuera de tolerancia. Una política más permisiva aumentaría la completitud, pero mantendría ventanas y estado activos durante más tiempo.

## Pipeline Apache Beam

`build_windowed_totals_pipeline` implementa el flujo:

```text
Create
  -> TimestampedValue(event_time)
  -> Filter(CONFIRMED)
  -> FixedWindows(60 s)
  -> KeyBy(merchant_id)
  -> CombinePerKey(sum)
  -> metadatos de ventana
```

La salida conserva:

- `merchant_id`;
- `window_start`;
- `window_end`;
- `total`.

## Deduplicación con estado

La deduplicación se implementa mediante un `DoFn` stateful.

La clave utilizada antes del estado es `merchant_id`. De esta forma, el mismo `event_id` puede existir en comercios diferentes sin producir una colisión.

Para cada comercio se mantiene un `SetStateSpec` con los `event_id` ya observados.

Si un evento ya se encuentra en el estado, no vuelve a emitirse.

## Expiración mediante timer

El estado no debe crecer indefinidamente.

Se utiliza un timer de tiempo de evento (`TimeDomain.WATERMARK`) que expira en:

```text
window_end + 120 segundos
```

Cuando el timer vence, se ejecuta `seen_ids.clear()`.

Esto mantiene el estado acotado y permite conservar la deduplicación durante todo el período en que todavía pueden llegar correcciones tardías.

## Triggers y panes

La política de ventanas para streaming utiliza:

- `AfterWatermark` para el pane ON-TIME;
- `AfterProcessingTime(10)` para una estimación EARLY;
- `AfterCount(1)` para emitir una revisión LATE por cada nuevo elemento tardío;
- `allowed_lateness=120`;
- `AccumulationMode.ACCUMULATING`.

Al utilizar panes acumulativos, cada nueva emisión representa el resultado actualizado de la ventana y no solamente el incremento desde el pane anterior.

## Prueba temporal con TestStream

Además de las pruebas provistas, se agregó `tests/test_teststream.py`.

La prueba utiliza `TestStream` para controlar explícitamente el watermark y demostrar un evento tardío aceptado.

Secuencia:

```text
event_time = 5   -> 100
event_time = 40  -> 70
watermark -> 60
event_time = 50  -> 20 (llega tarde)
```

El evento con timestamp 50 pertenece todavía a la ventana `[0, 60)`, aunque llega después de que el watermark cruzó su final.

Con panes acumulativos se observa:

```text
ON-TIME -> 170
LATE    -> 190
```

## Idempotencia y reintentos

La clave lógica del sink es:

```text
merchant_id|window_start
```

En modo append-only, cada intento agrega una nueva fila.

En modo idempotente, el resultado se almacena por su clave lógica y los reintentos convergen a una única entidad materializada. La auditoría conserva todos los intentos realizados.

## Trade-offs

### Lateness

Una lateness mayor mejora la completitud, pero prolonga la vida de ventanas y estado, aumentando memoria, cómputo y posibles correcciones. Se eligieron 120 segundos porque forman parte del contrato de la tarea.

### Panes EARLY

Los panes tempranos reducen la latencia percibida, pero generan más actualizaciones. Se utiliza una estimación EARLY después de 10 segundos de processing time.

### Estado

La deduplicación requiere recordar IDs observados. Sin expiración, ese estado podría crecer indefinidamente. El timer limita explícitamente su vida útil.

### Idempotencia

El modo append-only conserva todos los intentos, pero puede duplicar un efecto lógico. El UPSERT idempotente evita duplicados materiales ante reintentos.

## Reproducibilidad con uv

Instalar las dependencias exactas del proyecto:

```bash
uv sync --frozen
```

Ejecutar el notebook:

```bash
uv run marimo edit notebook.py
```

Ejecutar las pruebas:

```bash
uv run pytest
```

Validar estilo:

```bash
uv run ruff check .
```

Validar la estructura de Marimo:

```bash
uv run marimo check --strict notebook.py
```

## Reproducibilidad con Docker

Construir e iniciar el notebook:

```bash
docker compose up --build notebook
```

Luego abrir `http://localhost:2718`.

También pueden ejecutarse las pruebas dentro del contenedor:

```bash
docker compose exec notebook uv run pytest
```

## Evidencia de ejecución

Estado final verificado localmente:

```text
uv run pytest
14 passed
```

Las 14 pruebas corresponden a:

- 13 pruebas provistas por la tarea;
- 1 prueba adicional con `TestStream`.

Validación estática:

```text
uv run ruff check .
All checks passed!
```

Validación del notebook:

```text
uv run marimo check --strict notebook.py
exit code: 0
```

## Entrega

La entrega consiste en un único enlace público al repositorio de GitHub.

El archivo original `data/payments.jsonl` no fue modificado.
