# ── VPC for MWAA ──────────────────────────────────────────────────────────────
#
# MWAA requires private subnets with outbound internet access (NAT Gateway)
# so workers can install Python packages and reach the Comtrade API.
#
# All resources are conditional on var.enable_mwaa so the VPC is only
# provisioned in environments that run MWAA (staging, prod).
# In dev, Docker Compose is used instead.
# ─────────────────────────────────────────────────────────────────────────────

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # Slice to exactly 2 AZs for MWAA (it requires at least 2 private subnets).
  mwaa_azs             = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs = ["10.0.11.0/24", "10.0.12.0/24"]
}

# ── VPC ───────────────────────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  count = var.enable_mwaa ? 1 : 0

  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "${local.name_prefix}-vpc" }
}

# ── Internet Gateway ──────────────────────────────────────────────────────────

resource "aws_internet_gateway" "main" {
  count = var.enable_mwaa ? 1 : 0

  vpc_id = aws_vpc.main[0].id
  tags   = { Name = "${local.name_prefix}-igw" }
}

# ── Public subnets (NAT GW lives here) ───────────────────────────────────────

resource "aws_subnet" "public" {
  count = var.enable_mwaa ? 2 : 0

  vpc_id                  = aws_vpc.main[0].id
  cidr_block              = local.public_subnet_cidrs[count.index]
  availability_zone       = local.mwaa_azs[count.index]
  map_public_ip_on_launch = true

  tags = { Name = "${local.name_prefix}-public-${count.index + 1}" }
}

# ── Private subnets (MWAA workers run here) ───────────────────────────────────

resource "aws_subnet" "private" {
  count = var.enable_mwaa ? 2 : 0

  vpc_id            = aws_vpc.main[0].id
  cidr_block        = local.private_subnet_cidrs[count.index]
  availability_zone = local.mwaa_azs[count.index]

  tags = { Name = "${local.name_prefix}-private-${count.index + 1}" }
}

# ── NAT Gateway (single, in first public subnet — use two for HA in prod) ────

resource "aws_eip" "nat" {
  count = var.enable_mwaa ? 1 : 0

  domain     = "vpc"
  depends_on = [aws_internet_gateway.main]
  tags       = { Name = "${local.name_prefix}-nat-eip" }
}

resource "aws_nat_gateway" "main" {
  count = var.enable_mwaa ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.main]
  tags          = { Name = "${local.name_prefix}-nat" }
}

# ── Route tables ──────────────────────────────────────────────────────────────

resource "aws_route_table" "public" {
  count = var.enable_mwaa ? 1 : 0

  vpc_id = aws_vpc.main[0].id
  tags   = { Name = "${local.name_prefix}-public-rt" }
}

resource "aws_route" "public_internet" {
  count = var.enable_mwaa ? 1 : 0

  route_table_id         = aws_route_table.public[0].id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main[0].id
}

resource "aws_route_table_association" "public" {
  count = var.enable_mwaa ? 2 : 0

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table" "private" {
  count = var.enable_mwaa ? 1 : 0

  vpc_id = aws_vpc.main[0].id
  tags   = { Name = "${local.name_prefix}-private-rt" }
}

resource "aws_route" "private_nat" {
  count = var.enable_mwaa ? 1 : 0

  route_table_id         = aws_route_table.private[0].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main[0].id
}

resource "aws_route_table_association" "private" {
  count = var.enable_mwaa ? 2 : 0

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

# ── Security group for MWAA ───────────────────────────────────────────────────

resource "aws_security_group" "mwaa" {
  count = var.enable_mwaa ? 1 : 0

  name        = "${local.name_prefix}-mwaa"
  description = "MWAA workers — self-referencing ingress, unrestricted egress"
  vpc_id      = aws_vpc.main[0].id

  # Workers communicate with each other (Celery tasks, web server, scheduler).
  ingress {
    description = "Self-referencing (worker-to-worker)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  # Unrestricted egress so workers can reach S3, Secrets Manager, CloudWatch,
  # and the Comtrade API without maintaining an allowlist.
  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-mwaa-sg" }
}
