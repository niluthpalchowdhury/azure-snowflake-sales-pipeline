-- =============================================================================
-- Post-load: parse VARIANT columns (after write_pandas)
--
-- NOTE: the transformer already runs these updates itself as the final step of
-- each day load. This script is the standalone equivalent, for manual repair or
-- to rebuild VARIANT columns without re-running the pipeline.
--
-- Option A — invoices touched in this transform run (pass list from app):
--   AND INVOICE_NUMBER IN (:invoice_list)
--
-- Option B — all headers updated on a given extract day:
--   WHERE LAST_EXTRACTED_DAY = :target_day
-- =============================================================================

UPDATE SALES_DB.RAW.SALES_HEADER
SET
  TAGS = PARSE_JSON(TAGS::STRING),
  TAXES_DETAIL = PARSE_JSON(TAXES_DETAIL::STRING),
  OVERALL_REFUNDS_DETAIL = PARSE_JSON(OVERALL_REFUNDS_DETAIL::STRING),
  REPRINT_EVENTS_DETAIL = PARSE_JSON(REPRINT_EVENTS_DETAIL::STRING)
WHERE LAST_EXTRACTED_DAY = :target_day
  AND (
    TAGS IS NOT NULL
    OR TAXES_DETAIL IS NOT NULL
    OR OVERALL_REFUNDS_DETAIL IS NOT NULL
    OR REPRINT_EVENTS_DETAIL IS NOT NULL
  );

-- No-op today: SALES_INVOICE_HISTORY is never written by the pipeline.
-- Retained for when the per-invoice merge (docs/section-05) is implemented.
UPDATE SALES_DB.RAW.SALES_INVOICE_HISTORY
SET HEADER_SNAPSHOT = PARSE_JSON(HEADER_SNAPSHOT::STRING)
WHERE REEXTRACTION_REQUESTED_DAY = :target_day
  AND HEADER_SNAPSHOT IS NOT NULL;

UPDATE SALES_DB.RAW.SALES_CHARGES
SET CHARGE_TAXES_DETAIL = PARSE_JSON(CHARGE_TAXES_DETAIL::STRING)
WHERE LAST_EXTRACTED_DAY = :target_day
  AND CHARGE_TAXES_DETAIL IS NOT NULL;

UPDATE SALES_DB.RAW.SALES_ITEMS
SET
  LINE_TAXES_DETAIL = PARSE_JSON(LINE_TAXES_DETAIL::STRING),
  ITEM_LOG_DETAIL = PARSE_JSON(ITEM_LOG_DETAIL::STRING),
  BATCHES_DETAIL = PARSE_JSON(BATCHES_DETAIL::STRING)
WHERE LAST_EXTRACTED_DAY = :target_day
  AND (
    LINE_TAXES_DETAIL IS NOT NULL
    OR ITEM_LOG_DETAIL IS NOT NULL
    OR BATCHES_DETAIL IS NOT NULL
  );

UPDATE SALES_DB.RAW.SALES_ITEM_OPTIONS
SET OPTION_TAXES_DETAIL = PARSE_JSON(OPTION_TAXES_DETAIL::STRING)
WHERE LAST_EXTRACTED_DAY = :target_day
  AND OPTION_TAXES_DETAIL IS NOT NULL;
