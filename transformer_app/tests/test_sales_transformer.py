"""Unit tests for the sales transform.

`transform_sales` is the one pure function in the pipeline: a nested Rista invoice payload in,
thirteen flat row lists out, with no I/O, no database and no environment access. It is also the
single point where a silent regression would corrupt every downstream table, which makes it the
highest-value test target in the repository.

Run from `transformer_app`:  python -m pytest tests -v
"""

import json

import pytest

from transformers.sales_transformer import transform_sales

BUSINESS_DAY = "2026-05-28"


def payload(*records: dict, business_day: str = BUSINESS_DAY) -> dict:
    """Wrap records in the snapshot envelope the extractor writes to blob."""
    return {"extractType": "sales", "businessDay": business_day, "data": list(records)}


def invoice(**overrides: object) -> dict:
    """A minimal valid invoice record; override individual fields per test."""
    record = {
        "invoiceNumber": "INV-1001",
        "branchCode": "BR01",
        "status": "Closed",
        "netAmount": 250.0,
    }
    record.update(overrides)
    return record


# ---------------------------------------------------------------------------
# Invoice key handling
# ---------------------------------------------------------------------------

def test_invoice_number_is_upper_cased_and_stripped():
    result = transform_sales(payload(invoice(invoiceNumber="  inv-1001  ")))

    assert len(result.headers) == 1
    assert result.headers[0]["invoice_number"] == "INV-1001"


@pytest.mark.parametrize("bad_invoice_number", [None, "", "   "])
def test_records_without_an_invoice_number_are_dropped(bad_invoice_number):
    result = transform_sales(payload(invoice(invoiceNumber=bad_invoice_number)))

    assert result.headers == []


def test_child_rows_reuse_the_normalized_invoice_number():
    """A child row keyed on the raw string would silently fail to join to its header."""
    record = invoice(
        invoiceNumber="inv-1001",
        payments=[{"mode": "CASH", "amount": 250.0}],
        items=[{"itemNumber": 1, "shortName": "Burger"}],
    )

    result = transform_sales(payload(record))

    assert result.payments[0]["invoice_number"] == "INV-1001"
    assert result.items[0]["invoice_number"] == "INV-1001"


# ---------------------------------------------------------------------------
# Business-day provenance
# ---------------------------------------------------------------------------

def test_requested_day_falls_back_to_the_snapshot_business_day():
    """Older snapshots have no per-record requestedDay; the envelope value is used."""
    result = transform_sales(payload(invoice()))
    header = result.headers[0]

    assert header["last_extracted_day"] == BUSINESS_DAY
    assert header["requested_day"] == BUSINESS_DAY


def test_record_level_requested_day_wins_over_the_envelope():
    result = transform_sales(payload(invoice(requestedDay="2026-05-27")))
    header = result.headers[0]

    assert header["last_extracted_day"] == "2026-05-27"
    assert header["requested_day"] == "2026-05-27"


def test_requested_status_falls_back_to_the_live_status():
    result = transform_sales(payload(invoice(status="Closed")))
    assert result.headers[0]["requested_status"] == "Closed"

    result = transform_sales(payload(invoice(status="Closed", requestedStatus="Open")))
    assert result.headers[0]["requested_status"] == "Open"


# ---------------------------------------------------------------------------
# The upstream field-name typo
# ---------------------------------------------------------------------------

def test_direct_charge_amount_reads_the_upstream_typo():
    """The API field is spelled `directChargeAmout`."""
    result = transform_sales(payload(invoice(directChargeAmout=12.5)))

    assert result.headers[0]["direct_charge_amount"] == 12.5


def test_direct_charge_amount_accepts_the_corrected_spelling():
    """If the vendor ever fixes the typo, the column must not silently go null."""
    result = transform_sales(payload(invoice(directChargeAmount=12.5)))

    assert result.headers[0]["direct_charge_amount"] == 12.5


# ---------------------------------------------------------------------------
# VARIANT serialization and summary columns
# ---------------------------------------------------------------------------

def test_tax_array_is_serialized_and_summed():
    taxes = [{"name": "CGST", "amount": 6.25}, {"name": "SGST", "amount": 6.25}]

    header = transform_sales(payload(invoice(taxes=taxes))).headers[0]

    assert json.loads(header["taxes_detail"]) == taxes
    assert header["taxes_total_amount"] == 12.5


def test_empty_arrays_become_null_not_the_string_literal():
    """An empty list must land as SQL NULL, never as the two-character string "[]"."""
    header = transform_sales(payload(invoice(taxes=[], tags=[], overallRefunds=[]))).headers[0]

    assert header["taxes_detail"] is None
    assert header["tags"] is None
    assert header["overall_refunds_detail"] is None
    assert header["taxes_total_amount"] is None
    assert header["tag_count"] == 0
    assert header["reprint_events_count"] == 0


def test_refund_total_prefers_refund_amount_and_falls_back_to_amount():
    refunds = [{"refundAmount": 10.0}, {"amount": 5.0}]

    header = transform_sales(payload(invoice(overallRefunds=refunds))).headers[0]

    assert header["overall_refunds_total_amount"] == 15.0


def test_tax_entries_with_null_amounts_are_ignored_by_the_sum():
    taxes = [{"name": "CGST", "amount": 6.25}, {"name": "IGST", "amount": None}]

    header = transform_sales(payload(invoice(taxes=taxes))).headers[0]

    assert header["taxes_total_amount"] == 6.25


# ---------------------------------------------------------------------------
# Nested object flattening
# ---------------------------------------------------------------------------

def test_delivery_address_is_flattened_onto_the_delivery_row():
    record = invoice(
        delivery={
            "name": "Rider One",
            "mode": "DELIVERY",
            "address": {"addressLine": "12 Main St", "city": "Delhi", "zip": "110001"},
        }
    )

    delivery = transform_sales(payload(record)).deliveries[0]

    assert delivery["delivery_name"] == "Rider One"
    assert delivery["address_line"] == "12 Main St"
    assert delivery["address_city"] == "Delhi"
    assert delivery["address_zip"] == "110001"


def test_source_customer_detail_is_lifted_out_of_source_info():
    record = invoice(
        sourceInfo={
            "source": "AGGREGATOR",
            "sourceCustomerDetail": {"sourceCustomerId": "SC-9", "city": "Delhi"},
        }
    )

    result = transform_sales(payload(record))

    assert result.source_infos[0]["source"] == "AGGREGATOR"
    assert result.source_customers[0]["source_customer_id"] == "SC-9"
    assert result.source_customers[0]["city"] == "Delhi"


def test_absent_optional_objects_produce_no_satellite_rows():
    result = transform_sales(payload(invoice()))

    assert result.deliveries == []
    assert result.source_infos == []
    assert result.source_customers == []
    assert result.customers == []


# ---------------------------------------------------------------------------
# Child fan-out and sequence numbering
# ---------------------------------------------------------------------------

def test_repeating_children_are_numbered_from_one_in_source_order():
    record = invoice(
        charges=[{"name": "Packaging"}, {"name": "Delivery"}],
        discounts=[{"name": "Promo"}],
        payments=[{"mode": "CASH"}, {"mode": "CARD"}],
        eventLog=[{"status": "Created"}, {"status": "Closed"}],
    )

    result = transform_sales(payload(record))

    assert [row["charge_seq"] for row in result.charges] == [1, 2]
    assert [row["charge_name"] for row in result.charges] == ["Packaging", "Delivery"]
    assert [row["discount_seq"] for row in result.discounts] == [1]
    assert [row["payment_seq"] for row in result.payments] == [1, 2]
    assert [row["event_seq"] for row in result.event_logs] == [1, 2]


def test_line_items_fan_out_to_options_events_and_discounts():
    record = invoice(
        items=[
            {
                "itemNumber": 1,
                "shortName": "Burger",
                "quantity": 2,
                "options": [{"name": "Extra cheese"}, {"name": "No onion"}],
                "eventLog": [{"status": "Fired"}],
                "discounts": [{"name": "Combo"}],
                "taxes": [{"amount": 4.0}],
            }
        ]
    )

    result = transform_sales(payload(record))

    assert result.items[0]["item_number"] == 1
    assert result.items[0]["short_name"] == "Burger"
    assert result.items[0]["line_taxes_total_amount"] == 4.0
    assert [row["option_seq"] for row in result.item_options] == [1, 2]
    assert result.item_event_logs[0]["item_number"] == 1
    assert result.item_discounts[0]["discount_name"] == "Combo"


def test_line_children_are_keyed_to_their_own_item_number():
    """Options from item 2 must not be attributed to item 1."""
    record = invoice(
        items=[
            {"itemNumber": 1, "options": [{"name": "A"}]},
            {"itemNumber": 2, "options": [{"name": "B"}, {"name": "C"}]},
        ]
    )

    result = transform_sales(payload(record))

    by_item = {}
    for row in result.item_options:
        by_item.setdefault(row["item_number"], []).append(row["option_name"])

    assert by_item == {1: ["A"], 2: ["B", "C"]}


def test_line_without_an_item_number_is_dropped():
    """itemNumber is half of the SALES_ITEMS primary key, so a null cannot be loaded."""
    record = invoice(items=[{"itemNumber": None, "shortName": "Ghost"}, {"itemNumber": 1}])

    result = transform_sales(payload(record))

    assert [row["item_number"] for row in result.items] == [1]


def test_item_number_zero_is_kept():
    """Zero is a valid key value; only None is invalid."""
    record = invoice(items=[{"itemNumber": 0, "shortName": "Freebie"}])

    result = transform_sales(payload(record))

    assert [row["item_number"] for row in result.items] == [0]


# ---------------------------------------------------------------------------
# Batch-level invariants
# ---------------------------------------------------------------------------

def test_every_row_in_one_call_shares_a_single_load_timestamp():
    record = invoice(
        payments=[{"mode": "CASH"}],
        items=[{"itemNumber": 1, "options": [{"name": "A"}]}],
        customer={"name": "Guest"},
    )

    result = transform_sales(payload(record))
    stamps = {
        row["loaded_at"]
        for group in (result.headers, result.payments, result.items, result.item_options, result.customers)
        for row in group
    }

    assert len(stamps) == 1
    assert result.headers[0]["updated_at"] == result.headers[0]["loaded_at"]


def test_multiple_invoices_are_all_transformed():
    result = transform_sales(
        payload(
            invoice(invoiceNumber="INV-1001", items=[{"itemNumber": 1}]),
            invoice(invoiceNumber="INV-1002", items=[{"itemNumber": 1}, {"itemNumber": 2}]),
        )
    )

    assert [row["invoice_number"] for row in result.headers] == ["INV-1001", "INV-1002"]
    assert len(result.items) == 3


def test_empty_snapshot_yields_empty_row_lists():
    result = transform_sales(payload())

    assert result.headers == []
    assert result.items == []
    assert result.history_rows == []


def test_history_rows_are_never_populated():
    """The per-invoice merge that would fill SALES_INVOICE_HISTORY was never implemented.

    This test documents that gap rather than asserting desired behaviour: if the merge is built
    later, this test should be replaced, not deleted quietly.
    """
    record = invoice(items=[{"itemNumber": 1}], payments=[{"mode": "CASH"}])

    result = transform_sales(payload(record))

    assert result.history_rows == []
