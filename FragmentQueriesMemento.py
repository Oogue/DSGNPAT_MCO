import copy
from dataclasses import dataclass
from typing import Any, List, Dict

# Memento data class – immutable snapshot of the fragment aggregation progress
@dataclass
class FragmentQueriesMemento:
    node2_completed: bool
    node3_completed: bool
    node2_count: int
    node3_count: int
    rows_node2: List[Dict[str, Any]]
    rows_node3: List[Dict[str, Any]]

# Originator – the object whose state we save/restore during fragment queries
class FragmentQueriesState:
    def __init__(self):
        self.node2_completed = False
        self.node3_completed = False
        self.node2_count = 0
        self.node3_count = 0
        self.rows_node2: List[Dict[str, Any]] = []
        self.rows_node3: List[Dict[str, Any]] = []

    def save(self) -> FragmentQueriesMemento:
        # Capture a snapshot of current fragment state.
        return FragmentQueriesMemento(
            node2_completed=self.node2_completed,
            node3_completed=self.node3_completed,
            node2_count=self.node2_count,
            node3_count=self.node3_count,
            rows_node2=copy.deepcopy(self.rows_node2),
            rows_node3=copy.deepcopy(self.rows_node3)
        )

    def restore(self, memento: FragmentQueriesMemento) -> None:
        # Restore state from a snapshot.
        self.node2_completed = memento.node2_completed
        self.node3_completed = memento.node3_completed
        self.node2_count = memento.node2_count
        self.node3_count = memento.node3_count
        self.rows_node2 = copy.deepcopy(memento.rows_node2)
        self.rows_node3 = copy.deepcopy(memento.rows_node3)

    def mark_node2_success(self, count: int, rows: List) -> None:
        self.node2_completed = True
        self.node2_count = count
        self.rows_node2 = rows

    def mark_node3_success(self, count: int, rows: List) -> None:
        self.node3_completed = True
        self.node3_count = count
        self.rows_node3 = rows

# Caretaker - keeps track of the history/checkpoints of the fragment aggregation
class FragmentQueriesCaretaker:
    def __init__(self, originator: FragmentQueriesState):
        self._originator = originator
        self._history: List[FragmentQueriesMemento] = []

    def checkpoint(self) -> None:
        self._history.append(self._originator.save())

    def rollback(self) -> None:
        if self._history:
            self._originator.restore(self._history.pop())