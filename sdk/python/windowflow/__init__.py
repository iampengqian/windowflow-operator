# SPDX-License-Identifier: Apache-2.0
"""Window-level leases; release is always an explicit reader decision."""

from .client import (
    LeaseConflictError,
    LeaseReleasedError,
    PlanFailedError,
    PlanReplacedError,
    WindowClient,
    WindowFlowError,
    WindowHandle,
    WindowObserver,
    WindowPlanStatus,
    WindowSlotStatus,
    WindowTimeoutError,
    WindowValidationError,
    lease_name,
)
from .runner import WindowContext, WindowEpoch, WindowRunner
from .pytorch import TorchEpochFactory
from .schedule import ScheduleValidationError, build_plan, generate_schedule, validate_schedule

__version__ = "0.2.0a1"
__all__ = [
    "LeaseConflictError",
    "LeaseReleasedError",
    "PlanFailedError",
    "PlanReplacedError",
    "WindowClient",
    "WindowFlowError",
    "WindowHandle",
    "WindowObserver",
    "WindowPlanStatus",
    "WindowSlotStatus",
    "WindowContext",
    "WindowEpoch",
    "WindowRunner",
    "TorchEpochFactory",
    "WindowTimeoutError",
    "WindowValidationError",
    "lease_name",
    "build_plan",
    "generate_schedule",
    "validate_schedule",
    "ScheduleValidationError",
]
