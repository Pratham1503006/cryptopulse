"""Trust service: routes validated events from Bronze to Silver or Quarantine.

The application boundary of the trust milestone. It connects the three
architectural responsibilities without merging them:

    Bronze (received InternalEvents)
        -> ValidationEngine (trust decision, structured outcome)
        ├── valid   -> TrustedEvent -> Silver
        └── invalid -> Quarantine (structured failures preserved)

Responsibilities:

- read unvalidated events from Bronze,
- run the Validation Engine,
- route each event according to the structured outcome,
- preserve Bronze untouched in both paths (Bronze is the immutable record
  of what the platform received; rejection never deletes or modifies it).

Deliberate boundaries:

- No Gold, no aggregation (future Processing Engine).
- No Spark.
- No Kafka.
- No persistence logic of its own: it uses the warehouse repository
  abstractions only.

Failure semantics:

- An invalid event is a DATA-QUALITY OUTCOME, not an application error:
  it is persisted to Quarantine and processing continues.
- An infrastructure failure (StorageError from any repository) propagates
  to the caller and stops the batch. Failures are never misclassified as
  validation outcomes, and never silently swallowed.

Idempotency (at-least-once, no exactly-once claim):

- Valid events: Silver append is idempotent per technical event_id;
  reprocessing the same canonical event leaves Silver unchanged
  (validated_at stays the first-validation time).
- Invalid events: each processing attempt appends a DISTINCT quarantine
  record (identity (event_id, quarantined_at)), so repeated rejection
  attempts remain visible for investigation.
- Reprocessing a mixed batch is therefore deterministic: same valid
  events in Silver, one visible quarantine record per rejection attempt.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime

from common.contracts import InternalEvent, ValidationOutcome
from common.utils import get_logger
from processing.validation.base import Validator
from warehouse.repositories.base import BronzeRepository, QuarantineRepository, SilverRepository

logger = get_logger(__name__)

__all__ = ["TrustRoutingService", "TrustRoutingResult"]


@dataclass(frozen=True)
class TrustRoutingResult:
    """Summary of one trust-routing batch.

    Attributes:
        validated: InternalEvents that passed validation and were
            persisted to Silver.
        quarantined: InternalEvents that failed validation and were
            preserved in Quarantine with their structured failures.
        outcomes: The structured ValidationOutcome per quarantined event,
            in quarantine order (valid events have no stored outcome in
            Silver; their trust is expressed by their presence there).
    """

    validated: list[InternalEvent] = field(default_factory=list)
    quarantined: list[InternalEvent] = field(default_factory=list)
    outcomes: dict[str, ValidationOutcome] = field(default_factory=dict)


class TrustRoutingService:
    """Routes Bronze events through validation into Silver or Quarantine.

    Args:
        bronze: The Bronze repository abstraction (read-only usage).
        silver: The Silver repository abstraction.
        quarantine: The Quarantine repository abstraction.
        validator: The Validation Engine boundary.
    """

    def __init__(
        self,
        bronze: BronzeRepository,
        silver: SilverRepository,
        quarantine: QuarantineRepository,
        validator: Validator,
    ) -> None:
        self._bronze = bronze
        self._silver = silver
        self._quarantine = quarantine
        self._validator = validator

    async def process_batch(self, limit: int = 100) -> TrustRoutingResult:
        """Validate and route up to ``limit`` events from Bronze.

        Events are read from Bronze in its deterministic replay order. Each
        event is validated and routed individually; Bronze is never
        modified. The first infrastructure failure stops the batch and
        propagates; events already routed stay routed (at-least-once).

        Args:
            limit: Maximum number of Bronze events to process (>= 1).

        Returns:
            A TrustRoutingResult describing what was routed.

        Raises:
            ValueError: If ``limit`` is < 1.
            StorageError: If any repository operation fails.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        events: Sequence[InternalEvent] = await self._bronze.replay(offset=0, limit=limit)
        result = TrustRoutingResult()
        for event in events:
            outcome = self._validator.validate(event)
            if outcome.valid:
                await self._route_to_silver(event)
                result.validated.append(event)
            else:
                await self._route_to_quarantine(event, outcome)
                result.quarantined.append(event)
                result.outcomes[event.event_id] = outcome
        logger.info(
            "trust_routing_batch_complete",
            processed=len(events),
            validated=len(result.validated),
            quarantined=len(result.quarantined),
        )
        return result

    async def process_event(self, event: InternalEvent) -> ValidationOutcome:
        """Validate and route a single InternalEvent.

        Convenient for callers that already hold the event (e.g. tests or a
        future streaming driver). Same routing and failure semantics as
        :meth:`process_batch`.

        Returns:
            The structured ValidationOutcome for the event.

        Raises:
            StorageError: If a repository operation fails.
        """
        outcome = self._validator.validate(event)
        if outcome.valid:
            await self._route_to_silver(event)
        else:
            await self._route_to_quarantine(event, outcome)
        return outcome

    async def _route_to_silver(self, event: InternalEvent) -> None:
        """Persist the event as a canonical TrustedEvent in Silver."""
        trusted = self._validator.trusted_event_from(event)
        await self._silver.append(trusted)
        logger.debug("trust_routed_to_silver", event_id=event.event_id)

    async def _route_to_quarantine(
        self,
        event: InternalEvent,
        outcome: ValidationOutcome,
    ) -> None:
        """Preserve the event in Quarantine with its structured failures."""
        await self._quarantine.append_record(
            event=event,
            quarantined_at=datetime.now(UTC),
            failures=list(outcome.failures),
        )
        logger.debug(
            "trust_routed_to_quarantine",
            event_id=event.event_id,
            rules=[failure.rule for failure in outcome.failures],
        )
