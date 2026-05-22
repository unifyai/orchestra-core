import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path

import numpy as np


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.generic):
            return obj.item()
        elif isinstance(obj, time):
            return obj.strftime("%H:%M:%S.%f")
        elif isinstance(obj, date):
            # Handle both datetime and date objects
            if isinstance(obj, datetime):
                # Return ISO format with timezone info if available
                if obj.tzinfo is not None:
                    return obj.isoformat()
                return obj.replace(tzinfo=timezone.utc).isoformat()
            # For plain date objects
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            # Convert to ISO 8601 duration format
            # Format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
            total_seconds = obj.total_seconds()
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = total_seconds % 60

            # Build duration string
            duration = "P"
            if obj.days:
                duration += f"{obj.days}D"

            # Add time part if there are hours, minutes, or seconds
            if hours or minutes or seconds:
                duration += "T"
                if hours:
                    duration += f"{hours}H"
                if minutes:
                    duration += f"{minutes}M"
                if seconds:
                    # Handle fractional seconds
                    if seconds == int(seconds):
                        duration += f"{int(seconds)}S"
                    else:
                        duration += f"{seconds:g}S"  # :g removes trailing zeros

            # Handle zero duration edge case
            if duration == "P":
                duration = "PT0S"

            return duration
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, (set, frozenset)):
            return list(obj)
        elif isinstance(obj, (bytes, bytearray)):
            return obj.decode("utf-8", errors="replace")
        elif isinstance(obj, Enum):
            return obj.value
        elif is_dataclass(obj):
            return asdict(obj)

        return super().default(obj)
