"""Pydantic models for the rename-product-media job."""
from datetime import datetime
from typing import List

from pydantic import BaseModel, Field


class RenameSyncResult(BaseModel):
    """Aggregate statistics for one rename run."""

    total_products: int = Field(0, description="Products scanned")
    total_media: int = Field(0, description="Media nodes scanned")
    images_renamed: int = Field(0, description="Images whose filename and/or alt were updated")
    videos_alt_updated: int = Field(
        0, description="Videos and 3D models whose alt text was updated (filename not supported)"
    )
    skipped_already_ok: int = Field(0, description="Media already matching the target values")
    skipped_external_video: int = Field(0, description="External videos skipped (no filename concept)")
    skipped_not_ready: int = Field(0, description="Media skipped because status != READY")
    failed: int = Field(0, description="Media that failed to update")
    failed_media_ids: List[str] = Field(default_factory=list, description="GIDs of failed media")

    execution_time_seconds: float = Field(0.0, description="Total execution time")
    timestamp: datetime = Field(default_factory=datetime.now, description="When the run finished")
    dry_run: bool = Field(False, description="Whether this was a dry run")

    def is_success(self) -> bool:
        """A run is successful when nothing failed."""
        return self.failed == 0
