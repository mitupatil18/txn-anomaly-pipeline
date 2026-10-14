import uuid
from typing import Annotated

from confluent_kafka import KafkaException, Producer
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.engine import Connection

from txnpipe.api.deps import get_conn, get_producer
from txnpipe.api.routes.alerts import _alert_out
from txnpipe.api.schemas import Accepted, ErrorResponse, TransactionDetail, TransactionOut
from txnpipe.common.db import alerts, transactions
from txnpipe.common.schemas import TransactionEvent, event_to_json

router = APIRouter(tags=["transactions"])


@router.get(
    "/transactions/{transaction_id}",
    response_model=TransactionDetail,
    responses={404: {"model": ErrorResponse}},
)
def get_transaction(
    transaction_id: uuid.UUID, conn: Annotated[Connection, Depends(get_conn)]
) -> TransactionDetail:
    row = conn.execute(
        select(transactions).where(transactions.c.transaction_id == str(transaction_id))
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    rows = conn.execute(
        select(alerts)
        .where(alerts.c.transaction_id == str(transaction_id))
        .order_by(alerts.c.alert_id)
    ).all()
    base = TransactionOut(**{k: row._mapping[k] for k in TransactionOut.model_fields})
    return TransactionDetail(**base.model_dump(), alerts=[_alert_out(r) for r in rows])


@router.post("/transactions", status_code=202, response_model=Accepted)
def publish_transaction(
    event: TransactionEvent,
    request: Request,
    producer: Annotated[Producer, Depends(get_producer)],
) -> Accepted:
    """Validate a transaction and publish it to the raw topic (handy for manual demos)."""
    topic = request.app.state.settings.topic_raw
    try:
        producer.produce(topic, value=event_to_json(event), key=event.account_id)
        remaining = producer.flush(5)
    except (KafkaException, BufferError) as exc:
        raise HTTPException(status_code=503, detail="could not publish to Kafka") from exc
    if remaining:
        raise HTTPException(status_code=503, detail="Kafka publish timed out")
    return Accepted(transaction_id=str(event.transaction_id))
