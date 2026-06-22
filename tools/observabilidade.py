"""Instrumentacao do pipeline: logging JSON, metricas Prometheus e tabela de auditoria."""
import json
import logging
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("pipeline")

PUSHGATEWAY_HOST = "pushgateway:9091"
JOB_NAME = "pipeline_ecommerce"
METRICS_FILE_DIR = "/tmp"
MAX_RUN_HISTORY = 3
RUN_HISTORY_FILE = Path(METRICS_FILE_DIR) / "_pipeline_run_history.txt"
RUN_ID_FILE = Path(METRICS_FILE_DIR) / "pipeline_run_id.txt"

_gauge_fase_duration = None
_gauge_row_count = None
_gauge_camada_duration = None
_gauge_last_run_status = None


def _ensure_prometheus():
    global _gauge_fase_duration, _gauge_row_count, _gauge_camada_duration, _gauge_last_run_status
    if _gauge_fase_duration is not None:
        return
    from prometheus_client import Gauge

    _gauge_fase_duration = Gauge(
        "pipeline_fase_duration_seconds",
        "Duracao de cada fase do pipeline",
        ["camada", "tabela", "fase", "status"],
    )
    _gauge_row_count = Gauge(
        "pipeline_fase_row_count",
        "Linhas processadas por fase",
        ["camada", "tabela", "fase", "run_id"],
    )
    _gauge_camada_duration = Gauge(
        "pipeline_camada_duration_seconds",
        "Duracao total da camada",
        ["camada", "status"],
    )
    _gauge_last_run_status = Gauge(
        "pipeline_last_run_status",
        "Status da ultima run (0=sucesso, 1=erro)",
        ["camada"],
    )


def _push_to_gateway(camada):
    try:
        from prometheus_client import push_to_gateway
        push_to_gateway(PUSHGATEWAY_HOST, job=f"{JOB_NAME}_{camada}", registry=None)
    except Exception as e:
        print(f"[observabilidade] ERRO push ({camada}): {e}")


def _delete_pushgateway_run(run_id):
    for camada in ["bronze", "silver", "gold"]:
        try:
            url = f"http://{PUSHGATEWAY_HOST}/metrics/job/{JOB_NAME}_{camada}/label/run_id/{run_id}"
            req = urllib.request.Request(url, method="DELETE")
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass


def _cleanup_old_row_counts(run_id):
    history = []
    if RUN_HISTORY_FILE.exists():
        try:
            history = [l.strip() for l in RUN_HISTORY_FILE.read_text().strip().splitlines() if l.strip()]
        except Exception:
            pass
    if run_id not in history:
        history.insert(0, run_id)
    old_runs = history[MAX_RUN_HISTORY:]
    history = history[:MAX_RUN_HISTORY]
    RUN_HISTORY_FILE.write_text("\n".join(history))
    for old_id in old_runs:
        _delete_pushgateway_run(old_id)


def _log_json(camada, tabela, fase, duracao_ms, row_count=None, status="sucesso", erro=None):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "ERROR" if status == "erro" else "INFO",
        "camada": camada,
        "tabela": tabela,
        "fase": fase,
        "duracao_ms": duracao_ms,
        "status": status,
    }
    if row_count is not None:
        entry["row_count"] = row_count
    if erro:
        entry["erro_mensagem"] = erro
    print(json.dumps(entry, ensure_ascii=False))


def _gravar_audit(spark, run_id, camada, tabela, fase, duracao_ms, row_count, status, erro=None):
    try:
        audit_path = "/opt/spark/work-dir/warehouse/pipeline_audit"
        from pyspark.sql import Row

        row = Row(
            run_id=run_id,
            timestamp=datetime.now(timezone.utc),
            camada=camada,
            tabela=tabela,
            fase=fase,
            duracao_ms=duracao_ms,
            row_count=row_count or 0,
            status=status,
            erro_mensagem=erro or None,
        )
        df = spark.createDataFrame([row])
        df.write.format("delta").mode("append").save(audit_path)
    except Exception:
        pass


class RunContext:
    """Acompanha metricas de um run inteiro."""

    def __init__(self, camada, run_id=None):
        self.camada = camada
        self.run_id = run_id or self._shared_run_id()
        self.inicio = time.monotonic()
        self.fases = []
        self.teve_erro = False

    @staticmethod
    def _shared_run_id():
        if RUN_ID_FILE.exists():
            try:
                content = RUN_ID_FILE.read_text().strip()
                if content:
                    return content
            except Exception:
                pass
        run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
        RUN_ID_FILE.write_text(run_id)
        return run_id

    def registrar_fase(self, tabela, fase, duracao_ms, status, row_count=None):
        self.fases.append({
            "tabela": tabela,
            "fase": fase,
            "duracao_ms": duracao_ms,
            "row_count": row_count,
            "status": status,
        })
        if status == "erro":
            self.teve_erro = True

    def finalizar(self):
        duracao_total = (time.monotonic() - self.inicio) * 1000
        status = "erro" if self.teve_erro else "sucesso"
        _ensure_prometheus()
        _gauge_camada_duration.labels(camada=self.camada, status=status).set(duracao_total / 1000)
        _gauge_last_run_status.labels(camada=self.camada).set(1 if status == "erro" else 0)
        _cleanup_old_row_counts(self.run_id)
        _push_to_gateway(self.camada)
        _log_json(self.camada, "_total", "camada", duracao_total, status=status)
        metrics = {
            "run_id": self.run_id,
            "camada": self.camada,
            "duracao_total_ms": round(duracao_total),
            "status": status,
            "fases": self.fases,
        }
        metrics_path = Path(METRICS_FILE_DIR) / f"{self.camada}_metrics.json"
        metrics_path.write_text(json.dumps(metrics, ensure_ascii=False))
        return metrics


@contextmanager
def medir_fase(camada, tabela, fase, run_ctx=None, row_count=None, spark=None):
    """Mede tempo de uma fase, loga JSON, push para Pushgateway, grava audit."""
    _ensure_prometheus()
    inicio = time.monotonic()
    status = "sucesso"
    erro_msg = None
    try:
        yield
    except Exception as e:
        status = "erro"
        erro_msg = str(e)
        raise
    finally:
        duracao_ms = round((time.monotonic() - inicio) * 1000)
        _log_json(camada, tabela, fase, duracao_ms, row_count=row_count, status=status, erro=erro_msg)
        _gauge_fase_duration.labels(
            camada=camada, tabela=tabela, fase=fase, status=status
        ).set(duracao_ms / 1000)
        if row_count is not None:
            run_id = run_ctx.run_id if run_ctx else "standalone"
            _gauge_row_count.labels(
                camada=camada, tabela=tabela, fase=fase, run_id=run_id
            ).set(row_count)
        _push_to_gateway(camada)
        if run_ctx:
            run_ctx.registrar_fase(tabela, fase, duracao_ms, status, row_count)
        if spark and run_ctx:
            _gravar_audit(
                spark, run_ctx.run_id, camada, tabela, fase,
                duracao_ms, row_count, status, erro_msg,
            )
