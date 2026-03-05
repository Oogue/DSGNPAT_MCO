import copy
from dataclasses import dataclass
from typing import Any, List, Dict

# Memento data class – immutable snapshot
@dataclass
class RecoveryMemento:
    recovered_count: int
    recovery_logs: List[str]
    txn_statuses: Dict[str, str]

# Originator – the object whose state we save/restore
# the originator has access to all fields inside the memento, allowing it to restore its previous state at will.
class RecoveryState:
    def __init__(self):
        self.recovered_count = 0
        self.recovery_logs: List[str] = []
        self.txn_statuses: Dict[str, str] = {}

    def save(self) -> RecoveryMemento:
        # Capture a snapshot of current state.
        return RecoveryMemento(
            recovered_count=self.recovered_count,
            recovery_logs=copy.deepcopy(self.recovery_logs),
            txn_statuses=copy.deepcopy(self.txn_statuses)
        )

    def restore(self, memento: RecoveryMemento) -> None:
        # Restore state from a snapshot. Assign values from the data class
        self.recovered_count = memento.recovered_count
        self.recovery_logs = copy.deepcopy(memento.recovery_logs)
        self.txn_statuses = copy.deepcopy(memento.txn_statuses)

    def log_attempt(self, txn_id: str, target_node: str) -> None:
        self.recovery_logs.append(f"Recovering {txn_id} -> {target_node}...")

    def mark_success(self, txn_id: str, log_manager) -> None:
        self.txn_statuses[txn_id] = 'REPLICATION_SUCCESS'
        log_manager.update_replication_status(txn_id, 'REPLICATION_SUCCESS')
        self.recovery_logs.append("Success.")
        self.recovered_count += 1

    def mark_failure(self, txn_id: str, error: Any) -> None:
        self.txn_statuses[txn_id] = 'FAILED'
        self.recovery_logs.append(f"Failed: {error}")

# Caretaker - keeps track of the history of the originator, serving as the checkpoint
class RecoveryCaretaker:
    def __init__(self, originator: RecoveryState):
        self._originator = originator
        self._history: List[RecoveryMemento] = []

    def checkpoint(self) -> None:
        # Save current state before attempting an operation.
        self._history.append(self._originator.save())

    def rollback(self) -> None:
        # Revert to the last saved state (if any).
        if self._history:
            self._originator.restore(self._history.pop())