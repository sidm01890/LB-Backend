"""
Formula watcher scheduler
Periodically reads formulas from reco_logics.recologic and prints them.
"""

import asyncio
import json
import logging
import os
import hashlib
from typing import Optional, List, Dict, Any, Tuple, Iterable
from datetime import datetime

from sqlalchemy import text

from app.config import database as db_config

logger = logging.getLogger(__name__)

_watcher_task: Optional[asyncio.Task] = None
_stop_event: Optional[asyncio.Event] = None
_recalc_lock = asyncio.Lock()
_formula_hash_cache: Dict[int, str] = {}
_initialized = False

# Allow overriding via env var, default 10 seconds
FORMULA_WATCH_INTERVAL_SECONDS = int(os.getenv("FORMULA_WATCH_INTERVAL_SECONDS", "10"))


async def _fetch_recologics():
    """
    Fetch recologics records from main database (devyani).
    Only fetches records that:
    - Have recologic data
    - Have status 'UPDATED' or 'ACTIVE' (not yet processed)
    
    Note: effectivefrom and effectiveto are fetched for logging/info purposes only.
    The date range filtering is applied later when formulas are actually used in calculations,
    not during the processing phase.
    """
    if not db_config.main_session_factory:
        await db_config.create_engines()
    
    async with db_config.main_session_factory() as session:
        query = text("""
            SELECT id, tender, recologic, updated_date, status, effectivefrom, effectiveto
            FROM reco_logics
            WHERE recologic IS NOT NULL
                AND recologic != ''
                AND status IN ('UPDATED', 'ACTIVE')
            ORDER BY updated_date DESC
            LIMIT 50
        """)
        result = await session.execute(query)
        return result.fetchall()


def _extract_formulas(recologic_json: str) -> List[Dict[str, Any]]:
    """Parse recologic JSON string into formula list."""
    formulas = []
    
    if not recologic_json:
        return formulas
    
    try:
        data = json.loads(recologic_json)
        items = data if isinstance(data, list) else [data]
        
        for item in items:
            if not isinstance(item, dict):
                continue
            formulas.append({
                "logicName": item.get("logicName") or item.get("name"),
                "logicNameKey": item.get("logicNameKey"),
                "formulaText": item.get("formulaText") or item.get("expr"),
                "description": item.get("description"),
            })
    except json.JSONDecodeError as e:
        logger.error(f"[FORMULA_WATCHER] JSON parse error: {e}")
    
    return formulas


async def _formula_watcher_loop():
    """Background loop that fetches and prints formulas."""
    global _stop_event, _formula_hash_cache, _initialized
    logger.info(f"[FORMULA_WATCHER] Starting formula watcher loop (interval={FORMULA_WATCH_INTERVAL_SECONDS}s)")
    
    while not _stop_event.is_set():
        try:
            records = await _fetch_recologics()
            
            if not records:
                logger.info("[FORMULA_WATCHER] No reco_logics records found")
                _formula_hash_cache = {}
            else:
                changed_records, removed_ids = _detect_changes(records)
                
                # Process changed records: log them and mark as PROCESSED
                if changed_records:
                    processed_ids = []
                    for row in changed_records:
                        _log_formula_row(row)
                        if hasattr(row, 'id'):
                            processed_ids.append(row.id)
                    
                    # Mark formulas as PROCESSED after successful detection and logging
                    if processed_ids:
                        await _mark_as_processed(processed_ids)
                        logger.info(f"[FORMULA_WATCHER] Marked {len(processed_ids)} formulas as PROCESSED after detection")
                
                # Trigger recalculations if needed (after formulas are marked as processed)
                if (changed_records or removed_ids) and _initialized:
                    await _trigger_recalculations(changed_records, removed_ids)
                
                # Update cache after processing
                _formula_hash_cache = {
                    row.id: _hash_recologic(row.recologic)
                    for row in records
                }
                
                if not _initialized:
                    _initialized = True
            
        except Exception as e:
            logger.error(f"[FORMULA_WATCHER] Error during formula extraction: {e}", exc_info=True)
        
        try:
            await asyncio.wait_for(
                _stop_event.wait(),
                timeout=FORMULA_WATCH_INTERVAL_SECONDS
            )
        except asyncio.TimeoutError:
            continue
    
    logger.info("[FORMULA_WATCHER] Watcher loop stopped")


async def start_formula_watcher():
    """Start the formula watcher background task."""
    global _watcher_task, _stop_event
    
    if _watcher_task and not _watcher_task.done():
        logger.info("[FORMULA_WATCHER] Watcher already running")
        return
    
    _stop_event = asyncio.Event()
    _watcher_task = asyncio.create_task(_formula_watcher_loop(), name="formula_watcher")
    logger.info("[FORMULA_WATCHER] Watcher task created")


async def stop_formula_watcher():
    """Stop the formula watcher task."""
    global _watcher_task, _stop_event
    
    if _watcher_task:
        if _stop_event:
            _stop_event.set()
        await _watcher_task
        _watcher_task = None
        _stop_event = None
        logger.info("[FORMULA_WATCHER] Watcher task stopped")


def _hash_recologic(recologic: Optional[str]) -> str:
    """Return a stable hash for a recologic JSON string."""
    payload = recologic or ""
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _detect_changes(records) -> Tuple[List[Any], List[int]]:
    """Compare fetched records against cache and return (changed_rows, removed_ids)."""
    changed_records = []
    current_ids = set()
    
    for row in records:
        current_ids.add(row.id)
        current_hash = _hash_recologic(row.recologic)
        if _formula_hash_cache.get(row.id) != current_hash:
            changed_records.append(row)
    
    removed_ids = [rid for rid in _formula_hash_cache.keys() if rid not in current_ids]
    if removed_ids:
        logger.info("[FORMULA_WATCHER] Detected %d removed formula rows: %s", len(removed_ids), removed_ids)
    
    if changed_records:
        logger.info("[FORMULA_WATCHER] Detected %d updated/new formula rows", len(changed_records))
    
    return changed_records, removed_ids


def _log_formula_row(row):
    """Print/log formulas for a single reco_logics row."""
    formulas = _extract_formulas(row.recologic)
    printable = [
        {
            "logicName": f.get("logicName"),
            "logicNameKey": f.get("logicNameKey"),
            "formulaText": f.get("formulaText"),
        }
        for f in formulas
        if f.get("formulaText")
    ]
    
    message = {
        "id": row.id,
        "tender": row.tender,
        "updated_date": row.updated_date,
        "status": getattr(row, 'status', None),
        "effectivefrom": getattr(row, 'effectivefrom', None),
        "effectiveto": getattr(row, 'effectiveto', None),
        "formulas": printable,
    }
    print(f"[FORMULA_WATCHER] {json.dumps(message, default=str)}")
    logger.info("[FORMULA_WATCHER] %s", json.dumps(message, default=str))


async def _mark_as_processed(record_ids: List[int]):
    """Update status to 'PROCESSED' for successfully processed formula records."""
    if not record_ids:
        return
    
    try:
        if not db_config.main_session_factory:
            await db_config.create_engines()
        
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        async with db_config.main_session_factory() as session:
            # Build placeholders for IN clause
            placeholders = ",".join([f":id_{i}" for i in range(len(record_ids))])
            params = {f"id_{i}": record_id for i, record_id in enumerate(record_ids)}
            params["updated_date"] = current_timestamp
            
            # Update status to PROCESSED for all successfully processed records
            update_query = text(f"""
                UPDATE reco_logics
                SET status = 'PROCESSED',
                    updated_date = :updated_date
                WHERE id IN ({placeholders})
                    AND status IN ('UPDATED', 'ACTIVE')
            """)
            
            await session.execute(update_query, params)
            await session.commit()
            
            logger.info(f"[FORMULA_WATCHER] Marked {len(record_ids)} records as PROCESSED: {record_ids}")
    except Exception as exc:
        logger.error(f"[FORMULA_WATCHER] Failed to mark records as PROCESSED: {exc}", exc_info=True)


async def _trigger_recalculations(changed_rows: Iterable[Any], removed_ids: Iterable[int]):
    """Run the full calculation pipeline when formulas change."""
    if _recalc_lock.locked():
        logger.info("[FORMULA_WATCHER] Recalc already running, skipping trigger")
        return
    
    async with _recalc_lock:
        logger.info("[FORMULA_WATCHER] Triggering calculation pipeline due to formula update")
        
        try:
            if not db_config.main_session_factory:
                await db_config.create_engines()
            
            async with db_config.main_session_factory() as session:
                from app.routes.reconciliation import (
                    check_reconciliation_status,
                    prepare_self_reco_table,
                    prepare_cross_reco_table,
                )
                from app.routes.reconciliation import generate_common_trm_table_internal
                
                results = {}
                all_successful = True
                
                # 1. Populate 3PO dashboard
                try:
                    results["populate_threepo"] = await check_reconciliation_status(db=session, current_user=None)
                    if results["populate_threepo"].get("error"):
                        all_successful = False
                except Exception as exc:
                    logger.error("[FORMULA_WATCHER] Error running populate-threepo: %s", exc, exc_info=True)
                    results["populate_threepo"] = {"error": str(exc)}
                    all_successful = False
                
                # 2. Generate TRM table
                try:
                    await generate_common_trm_table_internal(session)
                    results["generate_trm"] = {"success": True}
                except Exception as exc:
                    logger.error("[FORMULA_WATCHER] Error generating TRM: %s", exc, exc_info=True)
                    results["generate_trm"] = {"error": str(exc)}
                    all_successful = False
                
                # 3. Prepare self-reco
                try:
                    results["prepare_self_reco"] = await prepare_self_reco_table(db=session, current_user=None)
                    if results["prepare_self_reco"].get("error"):
                        all_successful = False
                except Exception as exc:
                    logger.error("[FORMULA_WATCHER] Error preparing self-reco: %s", exc, exc_info=True)
                    results["prepare_self_reco"] = {"error": str(exc)}
                    all_successful = False
                
                # 4. Prepare cross-reco
                try:
                    results["prepare_cross_reco"] = await prepare_cross_reco_table(db=session, current_user=None)
                    if results["prepare_cross_reco"].get("error"):
                        all_successful = False
                except Exception as exc:
                    logger.error("[FORMULA_WATCHER] Error preparing cross-reco: %s", exc, exc_info=True)
                    results["prepare_cross_reco"] = {"error": str(exc)}
                    all_successful = False
                
                logger.info("[FORMULA_WATCHER] Calculation pipeline results: %s", json.dumps(results, default=str))
                
                if not all_successful:
                    logger.warning("[FORMULA_WATCHER] Some calculation pipeline steps failed, but formulas were already marked as PROCESSED")
        except Exception as exc:
            logger.error("[FORMULA_WATCHER] Failed to run calculation pipeline: %s", exc, exc_info=True)

