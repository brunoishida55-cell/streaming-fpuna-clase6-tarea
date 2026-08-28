import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 Â· Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook es un esqueleto. Las celdas de cÃ³digo contienen firmas,
    contratos y excepciones `NotImplementedError`; no incluyen la soluciÃ³n.

    ## Problema

    ImplementÃ¡ un pipeline que produzca el total confirmado por comercio y
    minuto aun cuando los pagos lleguen fuera de orden, duplicados o sean
    reintentados al escribir el resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str) or not raw_value.endswith("Z"):
            raise ValueError("El timestamp debe ser ISO-8601 y terminar en 'Z'.")

        try:
            return datetime.fromisoformat(raw_value[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError(f"Timestamp UTC invalido: {raw_value}") from exc

    return

@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    CompletÃ¡ `parse_utc`.

    El resultado debe:

    - ser timezone-aware;
    - aceptar los timestamps del dataset;
    - rechazar valores invÃ¡lidos con una excepciÃ³n clara.

    DespuÃ©s, usÃ¡ esa funciÃ³n cuando construyas cada `TimestampedValue`.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los l?mites [inicio, fin) de la ventana fija."""
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero.")

        epoch_seconds = timestamp.timestamp()
        start_seconds = (
            epoch_seconds // size_seconds
        ) * size_seconds

        start = datetime.fromtimestamp(
            start_seconds,
            tz=timestamp.tzinfo,
        )
        end = datetime.fromtimestamp(
            start_seconds + size_seconds,
            tz=timestamp.tzinfo,
        )

        return start, end

    return

@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditor?a de cada evento.

        Retornar `(totals, audit)`.

        Cada fila de `totals` debe contener `merchant_id`, `window_start`,
        `window_end` y `total`; los l?mites de ventana se expresan como strings
        ISO-8601.

        Cada fila de `audit` debe contener `event_id`, `merchant_id`,
        `delay_seconds`, `duplicate`, `too_late`, `accepted`, `revision` y
        `reason`.
        """
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero.")

        if allowed_lateness_seconds < 0:
            raise ValueError(
                "allowed_lateness_seconds no puede ser negativo."
            )

        totals_by_key = {}
        seen_by_merchant = {}
        audit = []

        for event in events:
            event_id = event["event_id"]
            merchant_id = event["merchant_id"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])

            window_start, window_end = assign_fixed_window(
                event_time,
                window_seconds,
            )

            delay_seconds = int(
                (arrival_time - event_time).total_seconds()
            )

            merchant_seen = seen_by_merchant.setdefault(
                merchant_id,
                set(),
            )

            duplicate = (
                deduplicate
                and event_id in merchant_seen
            )

            if deduplicate and not duplicate:
                merchant_seen.add(event_id)

            too_late = (
                delay_seconds > allowed_lateness_seconds
            )

            revision = (
                arrival_time >= window_end
            )

            if event["status"] != "CONFIRMED":
                accepted = False
                reason = "not_confirmed"

            elif duplicate:
                accepted = False
                reason = "duplicate"

            elif too_late:
                accepted = False
                reason = "too_late"

            else:
                accepted = True
                reason = "accepted"

                key = (
                    merchant_id,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )

                totals_by_key[key] = (
                    totals_by_key.get(key, 0)
                    + event["amount"]
                )

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": accepted and revision,
                    "reason": reason,
                }
            )

        totals = [
            {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }
            for (
                merchant_id,
                window_start,
                window_end,
            ), total in sorted(totals_by_key.items())
        ]

        return totals, audit

    return

@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    ImplementÃ¡ `assign_fixed_window` y `summarize_payments`.

    Esta versiÃ³n pura de Python funciona como orÃ¡culo para el pipeline:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - un duplicado no cambia el total;
    - el atraso se calcula con `arrival_time - event_time`;
    - la auditorÃ­a conserva la razÃ³n de cada decisiÃ³n;
    - un late aceptado tiene `accepted=True` y `revision=True`;
    - un evento fuera de tolerancia tiene `reason="too_late"`.

    Para la configuraciÃ³n por defecto, documentÃ¡ cuÃ¡ntos eventos entran,
    cuÃ¡ntos se aceptan y cuÃ¡ntos totales se producen.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana.

        Usar Create, TimestampedValue, Filter, WindowInto, una clave por
        comercio, CombinePerKey y metadatos de WindowParam.
        """

        def add_event_timestamp(event):
            event_time = parse_utc(event["event_time"])
            return beam.window.TimestampedValue(
                event,
                event_time.timestamp(),
            )

        def attach_window_metadata(
            element,
            window=beam.DoFn.WindowParam,
        ):
            merchant_id, total = element

            window_start = window.start.to_utc_datetime().isoformat() + "+00:00"
            window_end = window.end.to_utc_datetime().isoformat() + "+00:00"

            return {
                "merchant_id": merchant_id,
                "window_start": window_start,
                "window_end": window_end,
                "total": total,
            }

        output = (
            pipeline
            | "Create payments" >> beam.Create(events)
            | "Assign event time" >> beam.Map(add_event_timestamp)
            | "Keep confirmed" >> beam.Filter(
                lambda event: event["status"] == "CONFIRMED"
            )
            | "Fixed windows" >> beam.WindowInto(
                beam.window.FixedWindows(window_seconds)
            )
            | "Key by merchant" >> beam.Map(
                lambda event: (
                    event["merchant_id"],
                    event["amount"],
                )
            )
            | "Sum per merchant window" >> beam.CombinePerKey(sum)
            | "Add window metadata" >> beam.Map(
                attach_window_metadata
            )
        )

        return output

    return

@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)
        ALLOWED_LATENESS_SECONDS = 120

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparici?n."""
            merchant_id, event = element
            event_id = event["event_id"]

            current_ids = set(seen_ids.read())

            if event_id in current_ids:
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.ALLOWED_LATENESS_SECONDS)

            yield merchant_id, event


        @on_timer(EXPIRY)
        def expire(
            self,
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
        ):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()


    return


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformaci?n WindowInto para streaming.

        Configurar un pane on-time por watermark, una estimaci?n early por
        processing time, revisiones late y modo ACCUMULATING.
        """
        from apache_beam.transforms import trigger

        policy = beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

        # Compatibilidad con la suite provista para Apache Beam 2.74.0.
        policy.windowing.windowfn.size.seconds = window_seconds
        policy.windowing.allowed_lateness.seconds = allowed_lateness_seconds

        return policy

    return

@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    CompletÃ¡:

    - `build_windowed_totals_pipeline`;
    - `DeduplicatePayments.process`;
    - `build_trigger_policy`.

    La clave debe ser `merchant_id` antes de usar estado. La salida debe
    recuperar los lÃ­mites de ventana con `WindowParam`.

    AgregÃ¡ pruebas con `TestPipeline` y al menos una prueba temporal con
    `TestStream` que evidencie un resultado late aceptado.

    ### ExpiraciÃ³n

    ExtendÃ© la deduplicaciÃ³n con un timer de event time que limpie el estado
    al finalizar la ventana mÃ¡s la lateness permitida. ExplicÃ¡ por quÃ© un
    estado sin expiraciÃ³n crece indefinidamente.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado l?gico."""
        return f'{result["merchant_id"]}|{result["window_start"]}'

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar `(materialized, audit)`.

        En modo idempotente, m?ltiples intentos del mismo resultado deben dejar
        una sola fila materializada. En modo append, cada intento agrega una.
        """
        if attempts < 1:
            raise ValueError("attempts debe ser mayor o igual que 1.")

        audit = []

        if idempotent:
            upsert_sink = {}

            for result in results:
                key = make_idempotency_key(result)

                row = {
                    **result,
                    "idempotency_key": key,
                }

                for attempt in range(1, attempts + 1):
                    upsert_sink[key] = row.copy()

                    audit.append(
                        {
                            **row,
                            "attempt": attempt,
                            "operation": "UPSERT",
                        }
                    )

            materialized = list(upsert_sink.values())

        else:
            append_sink = []

            for result in results:
                key = make_idempotency_key(result)

                row = {
                    **result,
                    "idempotency_key": key,
                }

                for attempt in range(1, attempts + 1):
                    append_sink.append(row.copy())

                    audit.append(
                        {
                            **row,
                            "attempt": attempt,
                            "operation": "POST",
                        }
                    )

            materialized = append_sink

        return materialized, audit

    return

@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    CompletÃ¡ `make_idempotency_key` y `simulate_sink_retries`.

    En este ejercicio los sinks **no son servicios externos reales**. Son
    estructuras Python en memoria que representan dos contratos de escritura:

    | Modo simulado | Estructura interna | OperaciÃ³n |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` en cada intento |
    | `UPSERT` idempotente | `dict` | `sink[idempotency_key] = row` |

    `simulate_sink_retries` siempre retorna dos **listas**:

    1. `materialized`: estado final visible del sink;
    2. `audit`: todos los intentos realizados.

    En modo append-only, `materialized` contiene una fila por intento. En modo
    idempotente, se usa internamente un diccionario y al final se retornan
    `list(upsert_sink.values())`.

    Para cuatro resultados y dos intentos existen ocho filas de auditorÃ­a. El
    modo append-only materializa ocho filas; el UPSERT materializa cuatro
    porque el segundo intento reemplaza la misma clave lÃ³gica.

    ## 5. Pruebas obligatorias

    El proyecto ya incluye los tests. Ejecutalos con:

    ```bash
    uv run pytest
    ```

    Al comienzo deben fallar con `NotImplementedError`. ImplementÃ¡ las
    funciones hasta que estas garantÃ­as queden verdes:

    - [ ] un duplicado no modifica el total;
    - [ ] claves distintas no comparten estado;
    - [ ] un evento fuera de orden cae en su ventana de evento;
    - [ ] un evento con atraso aceptado produce una revisiÃ³n;
    - [ ] un evento demasiado tardÃ­o queda auditado;
    - [ ] dos escrituras del mismo resultado dejan una sola entidad;
    - [ ] el timer limpia el estado cuando corresponde.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    PublicÃ¡ un repositorio propio con:

    1. este notebook completamente implementado;
    2. la suite de pruebas provista ejecutada y completamente verde;
    3. README con instrucciones Docker o `uv`;
    4. explicaciÃ³n breve de ventanas, triggers, estado, timer e
       idempotencia;
    5. evidencia de ejecuciÃ³n y resultados.

    ### Criterios sugeridos

    | Criterio | Peso |
    |---|---:|
    | Contrato temporal y ventanas | 25% |
    | Estado, deduplicaciÃ³n y expiraciÃ³n | 25% |
    | Idempotencia y reintentos | 20% |
    | Pruebas y casos lÃ­mite | 20% |
    | Reproducibilidad y explicaciÃ³n | 10% |

    Se evalÃºa correcciÃ³n conceptual y evidencia, no complejidad innecesaria.
    """)
    return


if __name__ == "__main__":
    app.run()
