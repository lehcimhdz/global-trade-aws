# ── Glue Data Catalog database for Iceberg tables ─────────────────────────────
#
# PyIceberg writes Iceberg metadata to the Glue Data Catalog under the
# "comtrade" database.  One Glue table is created per Comtrade endpoint
# (e.g., comtrade.preview, comtrade.getMBS) with data files stored in
# s3://<bucket>/iceberg/<endpoint>/.

resource "aws_glue_catalog_database" "comtrade" {
  name        = "comtrade"
  description = "Apache Iceberg tables for Comtrade trade data"

  tags = local.tags
}
