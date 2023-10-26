#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type

from .utils import (
    insert_creditnote_without_inventory_to_tally,
    insert_debitnote_without_inventory_to_tally,
    insert_item_master_to_tally,
    insert_journal_voucher_to_tally,
    insert_ledger_master_to_tally,
    insert_payment_voucher_to_tally,
    insert_purchase_without_inventory_to_tally,
    insert_receipt_voucher_to_tally,
    insert_sales_order_to_tally,
    insert_sales_without_inventory_to_tally,
)


class DestinationTally(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        """
        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source

        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        logger = AirbyteLogger()
        for airbyte_message in input_messages:
            if airbyte_message.type == Type.RECORD:
                # check if airbyte stream contains any of supported_streams
                supported_streams = ["ledger", "item", "payment"]
                if not any(supported_stream in airbyte_message.record.stream for supported_stream in supported_streams):
                    logger.warn(
                        f"Skipping this stream : {airbyte_message.record.stream}, as it does not match any tally streams in [ledger, item, payment voucher]"
                    )
                    continue

                if "ledger" in airbyte_message.record.stream:
                    ledger_url = "https://api.excel2tally.in/api/User/LedgerMaster"
                    insert_ledger_master_to_tally(
                        config=config, data=airbyte_message.record.data, ledger_master_template_url=ledger_url, logger=logger
                    )
                elif "item" in airbyte_message.record.stream:
                    item_url = "https://api.excel2tally.in/api/User/ItemMaster"
                    insert_item_master_to_tally(
                        config=config, data=airbyte_message.record.data, item_master_template_url=item_url, logger=logger
                    )
                elif "payment" in airbyte_message.record.stream:
                    payment_voucher_url = "https://api.excel2tally.in/api/User/PaymentVoucher"
                    insert_payment_voucher_to_tally(
                        config=config, data=airbyte_message.record.data, payment_voucher_template_url=payment_voucher_url, logger=logger
                    )
                elif "sales_order" in airbyte_message.record.stream:
                    sales_order_url = "https://api.excel2tally.in/api/User/SalesOrder"
                    insert_sales_order_to_tally(
                        config=config, data=airbyte_message.record.data, sales_order_template_url=sales_order_url, logger=logger
                    )
                elif "purchase_without_inventory" in airbyte_message.record.stream:
                    purchase_without_inventory_url = "https://api.excel2tally.in/api/User/PurchaseWithoutInventory"
                    insert_purchase_without_inventory_to_tally(
                        config=config,
                        data=airbyte_message.record.data,
                        purchase_without_inventory_template_url=purchase_without_inventory_url,
                        logger=logger,
                    )
                elif "receipt" in airbyte_message.record.stream:
                    receipt_voucher_url = "https://api.excel2tally.in/api/User/ReceiptVoucher"
                    insert_receipt_voucher_to_tally(
                        config=config, data=airbyte_message.record.data, receipt_voucher_template_url=receipt_voucher_url, logger=logger
                    )
                elif "sales_without_inventory" in airbyte_message.record.stream:
                    sales_without_inventory_url = "https://api.excel2tally.in/api/User/SalesWithoutInventory"
                    insert_sales_without_inventory_to_tally(
                        config=config,
                        data=airbyte_message.record.data,
                        sales_without_inventory_template_url=sales_without_inventory_url,
                        logger=logger,
                    )
                elif "debitnote" in airbyte_message.record.stream:
                    debitnote_url = "https://api.excel2tally.in/api/User/DebitNoteWithoutInventory"
                    insert_debitnote_without_inventory_to_tally(
                        config=config,
                        data=airbyte_message.record.data,
                        debitnote_without_inventory_template_url=debitnote_url,
                        logger=logger,
                    )
                elif "journal" in airbyte_message.record.stream:
                    journal_voucher_url = "https://api.excel2tally.in/api/User/JournalTemplate"
                    insert_journal_voucher_to_tally(
                        config=config, data=airbyte_message.record.data, journal_voucher_template_url=journal_voucher_url, logger=logger
                    )
                elif "creditnote" in airbyte_message.record.stream:
                    credinote_without_inventory_url = "https://api.excel2tally.in/api/User/CreditNoteWithoutInventory"
                    insert_creditnote_without_inventory_to_tally(
                        config=config,
                        data=airbyte_message.record.data,
                        creditnote_without_inventory_template_url=credinote_without_inventory_url,
                        logger=logger,
                    )
            elif airbyte_message.type == Type.STATE:
                yield airbyte_message

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            if ("auth_key" not in config) or (config["auth_key"] == ""):
                logger.error("Please provide api auth key")
                return
            if ("company_name" not in config) or (config["company_name"] == ""):
                logger.error("Please provide company name.")
                return
            if ("version" not in config) or (config["version"] == ""):
                logger.error("Please provide version of tally prime software.")
                return
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
