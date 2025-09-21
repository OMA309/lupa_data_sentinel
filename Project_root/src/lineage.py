import datetime
import logging

def record_lineage(action: str, details: dict):
    """
    Records lineage actions (can later be connected to OpenLineage/Marquez).
    """
    log = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "action": action,
        "details": details
    }
    logging.info(f"DATA LINEAGE LOG: {log}")  # placeholder (later store in DB/lineage tool)