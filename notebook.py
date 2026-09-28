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
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    El objetivo es calcular el total confirmado por comercio y minuto, teniendo
    en cuenta eventos fuera de orden, duplicados y reintentos de escritura.

    ## Configuración usada
    1. `event_time` es el timestamp del dominio.
    2. Ventanas fijas de 60 segundos.
    3. Hasta 120 segundos de lateness.
    4. Deduplicación por `event_id` dentro de cada comercio.
    5. Panes acumulativos.
    6. Sink idempotente con clave `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 a un datetime UTC timezone-aware."""
        from datetime import UTC

        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError("El timestamp debe ser un string ISO-8601 no vacío")

        normalized = raw_value.strip()
        if normalized.endswith(("Z", "z")):
            normalized = f"{normalized[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"Timestamp ISO-8601 inválido: {raw_value!r}") from exc

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("El timestamp debe incluir zona horaria")

        return parsed.astimezone(UTC)

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` normaliza a UTC y rechaza timestamps sin zona horaria. De esta
    forma el pipeline puede asignar explícitamente el `event_time` mediante
    `TimestampedValue` y no depender del tiempo de llegada.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija en UTC."""
        from datetime import UTC
        from math import floor

        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("timestamp debe ser timezone-aware")
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser mayor que cero")

        timestamp_utc = timestamp.astimezone(UTC)
        start_epoch = floor(timestamp_utc.timestamp() / size_seconds) * size_seconds
        start = datetime.fromtimestamp(start_epoch, tz=UTC)
        end = datetime.fromtimestamp(start_epoch + size_seconds, tz=UTC)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Calcular los totales y registrar qué pasó con cada evento."""
        from datetime import timedelta

        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        seen_by_merchant: dict[str, set[str]] = {}
        totals_by_window: dict[tuple[str, str, str], int | float] = {}
        audit: list[dict[str, Any]] = []

        for event in events:
            event_id = str(event["event_id"])
            merchant_id = str(event["merchant_id"])
            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            window_start, window_end = assign_fixed_window(event_time, window_seconds)

            delay_seconds = (arrival_time - event_time).total_seconds()
            merchant_seen = seen_by_merchant.setdefault(merchant_id, set())
            duplicate = event_id in merchant_seen
            merchant_seen.add(event_id)

            # Se acepta un evento mientras no supere el fin de la ventana
            # más la tolerancia configurada.
            lateness_deadline = window_end + timedelta(seconds=allowed_lateness_seconds)
            too_late = arrival_time > lateness_deadline

            accepted = False
            revision = False

            if duplicate and deduplicate:
                reason = "duplicate"
            elif too_late:
                reason = "too_late"
            elif event.get("status") != "CONFIRMED":
                reason = "not_confirmed"
            else:
                accepted = True
                revision = arrival_time >= window_end
                reason = "accepted"

                key = (
                    merchant_id,
                    window_start.isoformat(),
                    window_end.isoformat(),
                )
                totals_by_window[key] = totals_by_window.get(key, 0) + event["amount"]

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
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
            for (merchant_id, window_start, window_end), total in sorted(
                totals_by_window.items(),
                key=lambda item: (item[0][1], item[0][0]),
            )
        ]
        return totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Reglas antes de armar el pipeline

    La función aplica estas mismas reglas:

    - solo cuenta pagos `CONFIRMED`;
    - la ventana depende de `event_time`;
    - la deduplicación está aislada por `merchant_id`;
    - `delay_seconds = arrival_time - event_time`;
    - un late aceptado se marca como revisión;
    - un evento que supera `window_end + allowed_lateness` se audita como
      `too_late`.
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
        """Construir la PCollection de totales confirmados por ventana."""
        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")

        class FormatTotal(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                merchant_id, total = element
                yield {
                    "merchant_id": merchant_id,
                    "window_start": window.start.to_utc_datetime(has_tz=True).isoformat(),
                    "window_end": window.end.to_utc_datetime(has_tz=True).isoformat(),
                    "total": total,
                }

        return (
            pipeline
            | "Create payments" >> beam.Create(events)
            | "Use event time"
            >> beam.Map(
                lambda event: beam.window.TimestampedValue(
                    event,
                    parse_utc(event["event_time"]).timestamp(),
                )
            )
            | "Only confirmed"
            >> beam.Filter(lambda event: event.get("status") == "CONFIRMED")
            | "Window per minute"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "Key amount"
            >> beam.Map(lambda event: (event["merchant_id"], event["amount"]))
            | "Sum per merchant and window" >> beam.CombinePerKey(sum)
            | "Attach window metadata" >> beam.ParDo(FormatTotal())
        )

    return (build_windowed_totals_pipeline,)


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
        """Evitar que un mismo event_id se procese dos veces."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def __init__(self, allowed_lateness_seconds: int = 120):
            if allowed_lateness_seconds < 0:
                raise ValueError("allowed_lateness_seconds no puede ser negativo")
            self.allowed_lateness_seconds = allowed_lateness_seconds

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el evento solamente la primera vez que aparece."""
            _, event = element
            event_id = str(event["event_id"])

            if event_id in set(seen_ids.read()):
                return

            seen_ids.add(event_id)
            expiry.set(window.end + self.allowed_lateness_seconds)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar los IDs guardados cuando vence el timer."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Configurar la ventana, triggers, lateness y acumulación."""
        from apache_beam.transforms import trigger

        if window_seconds <= 0:
            raise ValueError("window_seconds debe ser mayor que cero")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds no puede ser negativo")

        return beam.WindowInto(
            beam.window.FixedWindows(window_seconds),
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            allowed_lateness=allowed_lateness_seconds,
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        )

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    - `TimestampedValue` usa el `event_time` del pago.
    - `FixedWindows(60)` agrupa los pagos por minuto.
    - `AfterWatermark` permite emisiones early, on-time y late.
    - Se usa `ACCUMULATING` para que cada pane tenga el total actualizado.
    - `SetStateSpec` guarda los `event_id` ya vistos por comercio y ventana.
    - Un timer limpia ese estado cuando ya no hace falta conservarlo.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Crear la clave usada para evitar escrituras duplicadas."""
        try:
            merchant_id = result["merchant_id"]
            window_start = result["window_start"]
        except KeyError as exc:
            raise ValueError(f"Falta campo requerido para idempotencia: {exc.args[0]}") from exc
        return f"{merchant_id}|{window_start}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular reintentos de escritura usando POST o UPSERT."""
        if attempts < 0:
            raise ValueError("attempts no puede ser negativo")

        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        operation = "UPSERT" if idempotent else "POST"

        for result in results:
            idempotency_key = make_idempotency_key(result)
            materialized_row = {
                **result,
                "idempotency_key": idempotency_key,
            }

            for attempt in range(1, attempts + 1):
                audit.append(
                    {
                        **materialized_row,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

                if idempotent:
                    upsert_sink[idempotency_key] = dict(materialized_row)
                else:
                    append_sink.append(dict(materialized_row))

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return make_idempotency_key, simulate_sink_retries


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Escritura del resultado

    Para los reintentos se usa una clave formada por comercio y ventana. En modo
    `UPSERT`, un segundo intento actualiza la misma fila. En modo `POST`, en cambio,
    cada reintento agrega una fila nueva y puede generar duplicados.
    """)
    return


@app.cell
def _(mo, summarize_payments):
    import json
    from pathlib import Path

    data_path = Path("data/payments.jsonl")
    if data_path.exists():
        events = [
            json.loads(line)
            for line in data_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        totals, audit = summarize_payments(events)
        accepted = sum(row["accepted"] for row in audit)
        too_late = sum(row["too_late"] for row in audit)
        duplicates = sum(row["duplicate"] for row in audit)
        evidence = mo.md(
            f"""
            ## Resultado con el dataset base

            - eventos leídos: **{len(events)}**
            - aceptados con lateness=120 s: **{accepted}**
            - duplicados detectados: **{duplicates}**
            - eventos demasiado tardíos: **{too_late}**
            - filas de totales: **{len(totals)}**
            """
        )
    else:
        events, totals, audit = [], [], []
        evidence = mo.md("`data/payments.jsonl` no está disponible en este directorio.")

    evidence
    return audit, events, totals


@app.cell
def _(mo):
    mo.md(r"""
    ## Validación

    ```bash
    uv sync --frozen
    uv run pytest
    uv run ruff check notebook.py
    uv run marimo check --strict notebook.py
    ```

    Las pruebas cubren eventos desordenados, duplicados, datos tardíos, limpieza
    del estado y reintentos de escritura.
    """)
    return


if __name__ == "__main__":
    app.run()
