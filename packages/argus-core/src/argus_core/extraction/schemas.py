from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class StructuredMetadata(BaseModel):
    """Normalized metadata from HTML meta tags and JSON-LD."""

    open_graph: dict[str, str] = Field(default_factory=dict)
    meta_tags: dict[str, str] = Field(default_factory=dict)
    json_ld: list[dict] = Field(default_factory=list)
    schema_types: list[str] = Field(default_factory=list)


class PaginationHint(BaseModel):
    """Detected pagination patterns."""

    next_url: str | None = None
    prev_url: str | None = None
    page_numbers: list[int] = Field(default_factory=list)
    is_infinite_scroll: bool = False


class ExtractedPage(BaseModel):
    """Validated extraction output schema."""

    title: str | None = None
    description: str | None = None
    language: str | None = None
    text_snippet: str | None = None
    extracted_links: list[str] = Field(default_factory=list)
    metadata: StructuredMetadata = Field(default_factory=StructuredMetadata)
    pagination: PaginationHint = Field(default_factory=PaginationHint)
    content_type: str = "text/html"

    @field_validator("extracted_links")
    @classmethod
    def validate_links(cls, links: list[str]) -> list[str]:
        return [link for link in links if link.startswith(("http://", "https://"))]

    def to_legacy_dict(self) -> dict:
        """Compatibility with existing PageParsed metadata field."""
        legacy_meta: dict = {}
        legacy_meta.update(self.metadata.open_graph)
        legacy_meta.update(self.metadata.meta_tags)
        if self.metadata.schema_types:
            legacy_meta["schema_types"] = self.metadata.schema_types
        return {
            "title": self.title,
            "description": self.description,
            "language": self.language,
            "text_snippet": self.text_snippet,
            "extracted_links": self.extracted_links,
            "metadata": legacy_meta,
        }
