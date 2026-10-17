"""Loader module to parse and validate config/pipeline_config.yaml using Pydantic."""

from pathlib import Path
from typing import Dict, Optional
import yaml
from pydantic import BaseModel, Field, field_validator, ValidationError


class AWSConfig(BaseModel):
    s3_bucket: Optional[str] = "local-bucket"
    region: Optional[str] = "local"


class PipelineEnvConfig(BaseModel):
    environment: str = Field(..., min_length=1)


class AirportMetadata(BaseModel):
    iata_code: str = Field(..., min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    icao_code: str = Field(..., min_length=4, max_length=4, pattern=r"^[A-Z]{4}$")
    name: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)
    state: str = Field(..., min_length=1)
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)

    @field_validator("name", "city", "state")
    @classmethod
    def validate_non_whitespace(cls, v: str, info) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError(f"Field '{info.field_name}' cannot be blank.")
        return cleaned


class PipelineConfig(BaseModel):
    aws: Optional[AWSConfig] = Field(default_factory=AWSConfig)
    pipeline: PipelineEnvConfig
    airports: Dict[str, AirportMetadata]


def load_pipeline_config(config_path: str = "config/pipeline_config.yaml") -> PipelineConfig:
    """Read YAML file and return a validated PipelineConfig object."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path.resolve()}")

    with open(path, "r", encoding="utf-8") as f:
        raw_data = yaml.safe_load(f)

    try:
        config = PipelineConfig(**raw_data)
    except ValidationError as e:
        raise ValueError(f"Invalid pipeline configuration in {config_path}:\n{e}") from e

    # Validate key alignment
    for key, airport in config.airports.items():
        if key != airport.iata_code:
            raise ValueError(f"Key mismatch in config: YAML key '{key}' != iata_code '{airport.iata_code}'")

    return config


if __name__ == "__main__":
    cfg = load_pipeline_config()
    print(f"Successfully loaded configuration for env: {cfg.pipeline.environment}")
    print(f"Total Configured Airports: {len(cfg.airports)}")