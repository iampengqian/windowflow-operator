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
    WindowTimeoutError,
    WindowValidationError,
    lease_name,
)

__version__ = "0.1.0"
__all__ = [
    "LeaseConflictError",
    "LeaseReleasedError",
    "PlanFailedError",
    "PlanReplacedError",
    "WindowClient",
    "WindowFlowError",
    "WindowHandle",
    "WindowTimeoutError",
    "WindowValidationError",
    "lease_name",
]
