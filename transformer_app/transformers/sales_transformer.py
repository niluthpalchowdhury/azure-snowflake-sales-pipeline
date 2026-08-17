import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from shared.invoice_keys import normalize_invoice_number


def _json_value(value: Any) -> str | None:
    if value in (None, [], {}):
        return None
    return json.dumps(value, ensure_ascii=True)


def _sum_amount(items: list[dict], field_name: str = "amount") -> float | None:
    if not items:
        return None
    total = 0.0
    for item in items:
        amount = item.get(field_name)
        if amount is not None:
            total += float(amount)
    return total


def _sum_refund_amount(items: list[dict]) -> float | None:
    if not items:
        return None
    total = 0.0
    for item in items:
        amount = item.get("refundAmount")
        if amount is None:
            amount = item.get("amount")
        if amount is not None:
            total += float(amount)
    return total


def _parse_ts(value: Any) -> str | None:
    if not value:
        return None
    return str(value)


@dataclass
class SalesTransformResult:
    headers: list[dict] = field(default_factory=list)
    history_rows: list[dict] = field(default_factory=list)
    deliveries: list[dict] = field(default_factory=list)
    source_infos: list[dict] = field(default_factory=list)
    source_customers: list[dict] = field(default_factory=list)
    customers: list[dict] = field(default_factory=list)
    charges: list[dict] = field(default_factory=list)
    discounts: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)
    event_logs: list[dict] = field(default_factory=list)
    items: list[dict] = field(default_factory=list)
    item_options: list[dict] = field(default_factory=list)
    item_event_logs: list[dict] = field(default_factory=list)
    item_discounts: list[dict] = field(default_factory=list)


def transform_sales(raw_data: dict) -> SalesTransformResult:
    result = SalesTransformResult()
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    requested_day = raw_data.get("businessDay")

    for record in raw_data.get("data", []):
        invoice_number = normalize_invoice_number(record.get("invoiceNumber"))
        if not invoice_number:
            continue

        branch_code = (record.get("branchCode") or "").strip()
        last_extracted_day = record.get("requestedDay") or requested_day
        delivery = record.get("delivery") or {}
        delivery_address = delivery.get("address") or {}
        delivery_by = record.get("deliveryBy") or {}
        resource_info = record.get("resourceInfo") or {}
        customer = record.get("customer") or {}
        source_info = record.get("sourceInfo") or {}
        source_customer = source_info.get("sourceCustomerDetail") or {}
        status_info = record.get("statusInfo") or {}
        tags = record.get("tags") or []

        header = {
            "invoice_number": invoice_number,
            "branch_code": branch_code,
            "last_extracted_day": last_extracted_day,
            "requested_day": record.get("requestedDay") or last_extracted_day,
            "requested_status": record.get("requestedStatus") or record.get("status"),
            "branch_name": record.get("branchName"),
            "brand_name": record.get("brandName"),
            "branch_state": record.get("branchState"),
            "branch_tin": record.get("branchTIN"),
            "statement_number": record.get("statementNumber"),
            "invoice_date": _parse_ts(record.get("invoiceDate")),
            "invoice_day": record.get("invoiceDay"),
            "created_date": _parse_ts(record.get("createdDate")),
            "modified_date": _parse_ts(record.get("modifiedDate")),
            "device_label": record.get("deviceLabel"),
            "charge_tax_total": record.get("chargeTaxTotal"),
            "invoice_type": record.get("invoiceType"),
            "note": record.get("note"),
            "fulfillment_status": record.get("fulfillmentStatus"),
            "label": record.get("label"),
            "channel": record.get("channel"),
            "currency": record.get("currency"),
            "item_count": record.get("itemCount"),
            "person_count": record.get("personCount"),
            "kds_order_in_process": _parse_ts(record.get("kdsOrderInProcess")),
            "order_ready_timestamp": _parse_ts(record.get("orderReadyTimestamp")),
            "item_total_amount": record.get("itemTotalAmount"),
            "discount_amount": record.get("discountAmount"),
            "item_discount_amount": record.get("itemDiscountAmount"),
            "total_discount_amount": record.get("totalDiscountAmount"),
            "direct_charge_amount": record.get("directChargeAmout", record.get("directChargeAmount")),
            "charge_amount": record.get("chargeAmount"),
            "tax_amount_included": record.get("taxAmountIncluded"),
            "tax_amount_excluded": record.get("taxAmountExcluded"),
            "tax_amount": record.get("taxAmount"),
            "bill_amount": record.get("billAmount"),
            "gross_amount": record.get("grossAmount"),
            "net_discount_amount": record.get("netDiscountAmount"),
            "net_amount": record.get("netAmount"),
            "net_direct_charge_amount": record.get("netDirectChargeAmount"),
            "net_charge_amount": record.get("netChargeAmount"),
            "round_off_amount": record.get("roundOffAmount"),
            "bill_rounded_amount": record.get("billRoundedAmount"),
            "tip_amount": record.get("tipAmount"),
            "total_cost": record.get("totalCost"),
            "total_material_cost": record.get("totalMaterialCost"),
            "total_supplies_cost": record.get("totalSuppliesCost"),
            "balance_amount": record.get("balanceAmount"),
            "total_amount": record.get("totalAmount"),
            "tax_round_off": record.get("taxRoundOff"),
            "accounting_round_off": record.get("accountingRoundOff"),
            "reprint_count": record.get("reprintCount"),
            "url": record.get("url"),
            "status": record.get("status"),
            "sale_by": record.get("saleBy"),
            "sale_by_user_id": record.get("saleByUserId"),
            "delivery_by_name": delivery_by.get("name"),
            "delivery_by_phone": delivery_by.get("phoneNumber"),
            "resource_name": resource_info.get("resourceName"),
            "resource_group_size": resource_info.get("groupSize"),
            "status_info_reason": status_info.get("reason"),
            "status_info_remarks": status_info.get("remarks"),
            "tags": _json_value(tags),
            "tag_count": len(tags) if isinstance(tags, list) else 0,
            "taxes_detail": _json_value(record.get("taxes")),
            "taxes_total_amount": _sum_amount(record.get("taxes") or []),
            "overall_refunds_detail": _json_value(record.get("overallRefunds")),
            "overall_refunds_total_amount": _sum_refund_amount(record.get("overallRefunds") or []),
            "reprint_events_detail": _json_value(record.get("reprintEvents")),
            "reprint_events_count": len(record.get("reprintEvents") or []),
            "loaded_at": loaded_at,
            "updated_at": loaded_at,
        }
        result.headers.append(header)

        if delivery:
            result.deliveries.append(
                {
                    "invoice_number": invoice_number,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "delivery_name": delivery.get("name"),
                    "delivery_mode": delivery.get("mode"),
                    "delivery_date": _parse_ts(delivery.get("deliveryDate")),
                    "delivery_email": delivery.get("email"),
                    "delivery_phone_number": delivery.get("phoneNumber"),
                    "delivery_title": delivery.get("title"),
                    "delivery_payment_mode": delivery.get("paymentMode"),
                    "address_line": delivery_address.get("addressLine"),
                    "address_city": delivery_address.get("city"),
                    "address_state": delivery_address.get("state"),
                    "address_country": delivery_address.get("country"),
                    "address_zip": delivery_address.get("zip"),
                    "address_landmark": delivery_address.get("landmark"),
                    "address_label": delivery_address.get("label"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        if source_info:
            result.source_infos.append(
                {
                    "invoice_number": invoice_number,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "company_name": source_info.get("companyName"),
                    "source_invoice_number": source_info.get("invoiceNumber"),
                    "source_invoice_date": _parse_ts(source_info.get("invoiceDate")),
                    "source": source_info.get("source"),
                    "source_outlet_id": source_info.get("sourceOutletId"),
                    "outlet_id": source_info.get("outletId"),
                    "is_ecom_order": source_info.get("isEcomOrder"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        if source_customer:
            result.source_customers.append(
                {
                    "invoice_number": invoice_number,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "source_customer_id": source_customer.get("sourceCustomerId"),
                    "source": source_customer.get("source"),
                    "source_customer_name": source_customer.get("sourceCustomerName"),
                    "source_order_instructions": source_customer.get("sourceOrderInstructions"),
                    "pin_code": source_customer.get("pinCode"),
                    "delivery_coordinates_type": source_customer.get("deliveryCoordinatesType"),
                    "city": source_customer.get("city"),
                    "state": source_customer.get("state"),
                    "country": source_customer.get("country"),
                    "address_type": source_customer.get("addressType"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        if customer:
            result.customers.append(
                {
                    "invoice_number": invoice_number,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "customer_name": customer.get("name"),
                    "customer_title": customer.get("title"),
                    "customer_phone_number": customer.get("phoneNumber"),
                    "customer_id": customer.get("id"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        for charge_seq, charge in enumerate(record.get("charges") or [], start=1):
            result.charges.append(
                {
                    "invoice_number": invoice_number,
                    "charge_seq": charge_seq,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "charge_name": charge.get("name"),
                    "charge_type": charge.get("type"),
                    "charge_rate": charge.get("rate"),
                    "charge_sale_amount": charge.get("saleAmount"),
                    "charge_amount": charge.get("amount"),
                    "is_direct_charge": charge.get("isDirectCharge"),
                    "charge_tax_amount_included": charge.get("taxAmountIncluded"),
                    "charge_tax_amount_excluded": charge.get("taxAmountExcluded"),
                    "charge_tax_amount": charge.get("taxAmount"),
                    "charge_taxes_detail": _json_value(charge.get("taxes")),
                    "charge_taxes_total_amount": _sum_amount(charge.get("taxes") or []),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        for discount_seq, discount in enumerate(record.get("discounts") or [], start=1):
            result.discounts.append(
                {
                    "invoice_number": invoice_number,
                    "discount_seq": discount_seq,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "discount_name": discount.get("name"),
                    "discount_type": discount.get("type"),
                    "discount_rate": discount.get("rate"),
                    "discount_sale_amount": discount.get("saleAmount"),
                    "discount_amount": discount.get("amount"),
                    "loyalty_points": discount.get("loyaltyPoints"),
                    "applied_by": discount.get("appliedBy"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        for payment_seq, payment in enumerate(record.get("payments") or [], start=1):
            result.payments.append(
                {
                    "invoice_number": invoice_number,
                    "payment_seq": payment_seq,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "payment_mode": payment.get("mode"),
                    "payment_amount": payment.get("amount"),
                    "payment_reference": payment.get("reference"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        for event_seq, event in enumerate(record.get("eventLog") or [], start=1):
            result.event_logs.append(
                {
                    "invoice_number": invoice_number,
                    "event_seq": event_seq,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "event_status": event.get("status"),
                    "event_by_user_name": event.get("eventByUserName"),
                    "event_date": _parse_ts(event.get("eventDate")),
                    "event_note": event.get("note"),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

        for line in record.get("items") or []:
            item_number = line.get("itemNumber")
            if item_number is None:
                continue

            result.items.append(
                {
                    "invoice_number": invoice_number,
                    "item_number": item_number,
                    "last_extracted_day": last_extracted_day,
                    "branch_code": branch_code,
                    "short_name": line.get("shortName"),
                    "long_name": line.get("longName"),
                    "variants": line.get("variants"),
                    "sku_code": line.get("skuCode"),
                    "category_name": line.get("categoryName"),
                    "sub_category_name": line.get("subCategoryName"),
                    "brand_name": line.get("brandName"),
                    "quantity": line.get("quantity"),
                    "unit_price": line.get("unitPrice"),
                    "measuring_unit": line.get("measuringUnit"),
                    "item_nature": line.get("itemNature"),
                    "item_amount": line.get("itemAmount"),
                    "option_amount": line.get("optionAmount"),
                    "tax_code": line.get("taxCode"),
                    "discount_amount": line.get("discountAmount"),
                    "factored_discount_amount": line.get("factoredDiscountAmount"),
                    "item_total_amount": line.get("itemTotalAmount"),
                    "gross_amount": line.get("grossAmount"),
                    "net_discount_amount": line.get("netDiscountAmount"),
                    "net_amount": line.get("netAmount"),
                    "tax_amount_included": line.get("taxAmountIncluded"),
                    "tax_amount_excluded": line.get("taxAmountExcluded"),
                    "tax_amount": line.get("taxAmount"),
                    "item_cost": line.get("itemCost"),
                    "item_material_cost": line.get("itemMaterialCost"),
                    "item_supplies_cost": line.get("itemSuppliesCost"),
                    "note": line.get("note"),
                    "created_time": _parse_ts(line.get("createdTime")),
                    "created_by": line.get("createdBy"),
                    "kot_number": line.get("kotNumber"),
                    "aggregator_image_url": line.get("aggregatorImageURL"),
                    "base_gross_amount": line.get("baseGrossAmount"),
                    "base_net_discount_amount": line.get("baseNetDiscountAmount"),
                    "base_net_amount": line.get("baseNetAmount"),
                    "base_tax_amount": line.get("baseTaxAmount"),
                    "kot_status": line.get("kotStatus"),
                    "kot_timestamp": _parse_ts(line.get("kotTimestamp")),
                    "kds_ready": _parse_ts(line.get("kdsReady")),
                    "line_taxes_detail": _json_value(line.get("taxes")),
                    "line_taxes_total_amount": _sum_amount(line.get("taxes") or []),
                    "item_log_detail": _json_value(line.get("itemLog")),
                    "batches_detail": _json_value(line.get("batches")),
                    "loaded_at": loaded_at,
                    "updated_at": loaded_at,
                }
            )

            for option_seq, option in enumerate(line.get("options") or [], start=1):
                result.item_options.append(
                    {
                        "invoice_number": invoice_number,
                        "item_number": item_number,
                        "option_seq": option_seq,
                        "last_extracted_day": last_extracted_day,
                        "branch_code": branch_code,
                        "option_type": option.get("type"),
                        "option_id": option.get("optionId"),
                        "option_name": option.get("name"),
                        "option_item_name": option.get("itemName"),
                        "option_variants": option.get("variants"),
                        "option_sku_code": option.get("skuCode"),
                        "option_category_name": option.get("categoryName"),
                        "option_brand_name": option.get("brandName"),
                        "option_quantity": option.get("quantity"),
                        "option_unit_price": option.get("unitPrice"),
                        "option_amount": option.get("amount"),
                        "option_tax_code": option.get("taxCode"),
                        "option_gross_amount": option.get("grossAmount"),
                        "option_net_discount_amount": option.get("netDiscountAmount"),
                        "option_net_amount": option.get("netAmount"),
                        "option_tax_amount": option.get("taxAmount"),
                        "option_taxes_detail": _json_value(option.get("taxes")),
                        "option_taxes_total_amount": _sum_amount(option.get("taxes") or []),
                        "loaded_at": loaded_at,
                        "updated_at": loaded_at,
                    }
                )

            for event_seq, event in enumerate(line.get("eventLog") or [], start=1):
                result.item_event_logs.append(
                    {
                        "invoice_number": invoice_number,
                        "item_number": item_number,
                        "event_seq": event_seq,
                        "last_extracted_day": last_extracted_day,
                        "branch_code": branch_code,
                        "event_status": event.get("status"),
                        "event_by_user_name": event.get("eventByUserName"),
                        "event_date": _parse_ts(event.get("eventDate")),
                        "event_note": event.get("note"),
                        "loaded_at": loaded_at,
                        "updated_at": loaded_at,
                    }
                )

            for discount_seq, discount in enumerate(line.get("discounts") or [], start=1):
                result.item_discounts.append(
                    {
                        "invoice_number": invoice_number,
                        "item_number": item_number,
                        "discount_seq": discount_seq,
                        "last_extracted_day": last_extracted_day,
                        "branch_code": branch_code,
                        "discount_name": discount.get("name"),
                        "discount_type": discount.get("type"),
                        "discount_rate": discount.get("rate"),
                        "discount_sale_amount": discount.get("saleAmount"),
                        "discount_amount": discount.get("amount"),
                        "applied_by": discount.get("appliedBy"),
                        "loaded_at": loaded_at,
                        "updated_at": loaded_at,
                    }
                )

    return result
