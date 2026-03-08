# =============================================================================
# CRE Market Intelligence Platform — Infrastructure as Code
# Provider: kreuzwerker/docker (the canonical Terraform Docker provider)
# Idempotent: `terraform apply` can be run multiple times safely.
# =============================================================================

terraform {
  required_version = ">= 1.3.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  # Connects to the local Docker daemon.
  # On Linux: unix:///var/run/docker.sock
  # On Mac/Windows with Docker Desktop: tcp://localhost:2375 (or leave default)
  host = "npipe:////.//pipe//docker_engine"
}

# =============================================================================
# VARIABLES — All sensitive values are injected from .env / TF_VAR_ prefixes.
# Never hardcode credentials. Run: export TF_VAR_db_password="yourpassword"
# =============================================================================

variable "db_name" {
  description = "PostgreSQL database name for the CRE platform"
  type        = string
  default     = "cre_db"
}

variable "db_user" {
  description = "PostgreSQL superuser username"
  type        = string
  default     = "cre_user"
}

variable "db_password" {
  description = "PostgreSQL superuser password — inject via TF_VAR_db_password"
  type        = string
  sensitive   = true
}

variable "postgres_port" {
  description = "Host port to expose PostgreSQL on"
  type        = number
  default     = 5432
}

variable "mage_port" {
  description = "Host port to expose Mage.ai on"
  type        = number
  default     = 6789
}

# =============================================================================
# NETWORK — Isolated bridge network so containers resolve each other by name.
# Mage.ai connects to Postgres via hostname "cre_postgres" (container name).
# =============================================================================

resource "docker_network" "cre_network" {
  name   = "cre_platform_network"
  driver = "bridge"

  lifecycle {
    ignore_changes = [name]
  }
}

# =============================================================================
# IMAGES — Pull only if not already present locally (idempotent by default).
# =============================================================================

resource "docker_image" "postgres" {
  name         = "postgres:15-alpine"
  keep_locally = true # Don't delete the image on `terraform destroy`
}

resource "docker_image" "mage_ai" {
  name         = "mageai/mageai:latest"
  keep_locally = true
}

# =============================================================================
# VOLUME — Named volume for PostgreSQL data persistence.
# Data survives `terraform apply` re-runs and container restarts.
# =============================================================================

resource "docker_volume" "postgres_data" {
  name = "cre_postgres_data"
}

# =============================================================================
# CONTAINER: PostgreSQL 15
# =============================================================================

resource "docker_container" "postgres" {
  name  = "cre_postgres"
  image = docker_image.postgres.image_id
  restart = "unless-stopped"

  # CORRECTED: Points to the init.sql inside your 'postgres' folder
  volumes {
    host_path      = abspath("${path.module}/postgres/init.sql")
    container_path = "/docker-entrypoint-initdb.d/01_init.sql"
    read_only      = true
  }

  env = [
    "POSTGRES_DB=${var.db_name}",
    "POSTGRES_USER=${var.db_user}",
    "POSTGRES_PASSWORD=${var.db_password}",
    "POSTGRES_INITDB_ARGS=--encoding=UTF8 --lc-collate=en_US.utf8 --lc-ctype=en_US.utf8",
  ]

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }

  ports {
    internal = 5432
    external = var.postgres_port
  }

  networks_advanced {
    name = docker_network.cre_network.name
  }

  healthcheck {
    test         = ["CMD-SHELL", "pg_isready -U ${var.db_user} -d ${var.db_name}"]
    interval     = "10s"
    timeout      = "5s"
    retries      = 5
    start_period = "15s"
  }

  lifecycle {
    ignore_changes = [image]
  }
}

# =============================================================================
# CONTAINER: Mage.ai Orchestration Engine
# =============================================================================

resource "docker_container" "mage_ai" {
  name  = "cre_mage"
  image = docker_image.mage_ai.image_id

  restart = "unless-stopped"

  # Without this command, Mage ignores MAGE_PROJECT_NAME and boots
  # into default_repo. This explicitly tells Mage which project to start.
  command = ["mage", "start", "cre_platform"]

  # Pass DB connection details as env vars.
  # Mage pipelines reference these via os.environ — no credentials in code.
  env = [
    "MAGE_PROJECT_NAME=cre_platform",

    # PostgreSQL connection — uses the CONTAINER NAME as hostname (internal DNS)
    "POSTGRES_HOST=cre_postgres",
    "POSTGRES_PORT=5432",
    "POSTGRES_DB=${var.db_name}",
    "POSTGRES_USER=${var.db_user}",
    "POSTGRES_PASSWORD=${var.db_password}",

    # Mage dev server config
    "ENV=dev",
  ]

  # Mount 1: Mage project files (pipelines, configs)
  volumes {
    host_path      = abspath("${path.module}/mage_ai")
    container_path = "/home/src/cre_platform"
  }

  # Mount 2: Local CSV data (Stored INSIDE cre-market-platform/data)
  volumes {
    host_path      = abspath("${path.module}/data")
    container_path = "/home/src/data"
  }

  # Expose Mage UI to the host browser
  ports {
    internal = 6789
    external = var.mage_port
  }

  # Attach to the same internal network as Postgres
  networks_advanced {
    name = docker_network.cre_network.name
  }

  # Mage must wait for Postgres to be healthy before starting
  depends_on = [docker_container.postgres]

  lifecycle {
    ignore_changes = [image]
  }
}

# =============================================================================
# OUTPUTS — Useful connection strings printed after `terraform apply`
# =============================================================================

output "mage_ui_url" {
  description = "Open this URL in your browser to access the Mage.ai pipeline UI"
  value       = "http://localhost:${var.mage_port}"
}

output "postgres_connection_string" {
  description = "Use this DSN in DBeaver, psql, or dbt profiles.yml"
  value       = "postgresql://${var.db_user}:***@localhost:${var.postgres_port}/${var.db_name}"
  sensitive   = false
}

output "internal_postgres_host" {
  description = "Hostname Mage uses to reach Postgres INSIDE the Docker network"
  value       = "cre_postgres"
}
