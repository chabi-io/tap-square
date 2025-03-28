import os
from collections import namedtuple
import singer
import tap_tester.runner      as runner
import tap_tester.connections as connections

from base import TestSquareBaseParent, DataType
LOGGER = singer.get_logger()

PaymentRecordDetails = namedtuple('PaymentRecordDetails', 'source_key, autocomplete, record')


class TestSquareAllFields(TestSquareBaseParent.TestSquareBase):
    """Test that with all fields selected for a stream we replicate data as expected"""
    TESTABLE_STREAMS = set()

    def testable_streams_dynamic(self):
        return self.dynamic_data_streams().difference(self.untestable_streams())

    def testable_streams_static(self):
        return self.static_data_streams().difference(self.untestable_streams())

    def ensure_dict_object(self, resp_object):
        """
        Ensure the response object is a dictionary and return it.
        If the object is a list, ensure the list contains only one dict item and return that.
        Otherwise fail the test.
        """
        if isinstance(resp_object, dict):
            return resp_object
        elif isinstance(resp_object, list):
            self.assertEqual(1, len(resp_object),
                             msg="Multiple objects were returned, but only 1 was expected")
            self.assertTrue(isinstance(resp_object[0], dict),
                            msg="Response object is a list of {} types".format(type(resp_object[0])))
            return resp_object[0]
        else:
            raise RuntimeError("Type {} was unexpected.\nRecord: {} ".format(type(resp_object), resp_object))

    def create_specific_payments(self):
        """Create a record using each source type, and a record that will autocomplete."""
        LOGGER.info('Creating a record using each source type, and the autocomplete flag.')
        payment_records = []
        descriptions = {
            ("card", False),
            ("card_on_file", False),
            ("gift_card", False),
            ("card", True),
        }
        for source_key, autocomplete in descriptions:
            payment_response = self.client.create_payment(autocomplete=autocomplete, source_key=source_key)
            payment_record = PaymentRecordDetails(source_key, autocomplete, self.ensure_dict_object(payment_response))
            payment_records.append(payment_record)

        return payment_records

    def update_specific_payments(self, payments_to_update):
        """Perform specifc updates on specific payment records."""
        updated_records = []
        LOGGER.info('Updating payment records by completing, canceling and refunding them.')
        # Update a completed payment by making a refund (payments must have a status of 'COMPLETED' to process a refund)
        source_key, autocomplete = ("card", True)
        description = "refund"
        payment_to_update = [payment.record for payment in payments_to_update if payment.source_key == source_key and payment.autocomplete == autocomplete][0]
        _, payment_response = self.client.create_refund(self.START_DATE, payment_to_update)
        payment_record = PaymentRecordDetails(source_key, autocomplete, self.ensure_dict_object(payment_response))
        updated_records.append(payment_record)

        # Update a payment by completing it
        source_key, autocomplete = ("card_on_file", False)
        description = "complete"
        payment_to_update = [payment.record for payment in payments_to_update if payment.source_key == source_key and payment.autocomplete == autocomplete][0]
        payment_response = self.client.update_payment(payment_to_update.get('id'), action=description)
        payment_record = PaymentRecordDetails(source_key, autocomplete, self.ensure_dict_object(payment_response))
        updated_records.append(payment_record)

        # Update a payment by canceling it
        source_key, autocomplete = ("gift_card", False)
        description = "cancel"
        payment_to_update = [payment.record for payment in payments_to_update if payment.source_key == source_key and payment.autocomplete == autocomplete][0]
        payment_response = self.client.update_payment(payment_to_update.get('id'), action=description)
        payment_record = PaymentRecordDetails(source_key, autocomplete, self.ensure_dict_object(payment_response))
        updated_records.append(payment_record)

        return updated_records

    @classmethod
    def tearDownClass(cls):
        LOGGER.info('\n\nTEST TEARDOWN\n\n')

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""
        LOGGER.info('\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.DYNAMIC)

        LOGGER.info('\n\nTESTING WITH STATIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.STATIC_START_DATE
        self.TESTABLE_STREAMS = self.testable_streams_static().difference(self.production_streams())
        self.all_fields_test(self.SANDBOX, DataType.STATIC)

        TestSquareBaseParent.TestSquareBase.test_name = self.TEST_NAME_PROD
        self.set_environment(self.PRODUCTION)

        LOGGER.info('\n\nTESTING WITH DYNAMIC DATA IN SQUARE_ENVIRONMENT: {}'.format(os.getenv('TAP_SQUARE_ENVIRONMENT')))
        self.START_DATE = self.get_properties().get('start_date')
        self.TESTABLE_STREAMS = self.testable_streams_dynamic().difference(self.sandbox_streams())
        self.all_fields_test(self.PRODUCTION, DataType.DYNAMIC)
        TestSquareBaseParent.TestSquareBase.test_name = self.TEST_NAME_SANDBOX

    def all_fields_test(self, environment, data_type):
        """
        Verify that for each stream you can get data when no fields are selected
        and only the automatic fields are replicated.
        """

        LOGGER.info('\n\nRUNNING {}_all_fields'.format(self.name()))
        LOGGER.info('WITH STREAMS: {}\n\n'.format(self.TESTABLE_STREAMS))

        # execute specific creates and updates for the payments stream in addition to the standard create
        if 'payments' in self.TESTABLE_STREAMS:
            created_payments = self.create_specific_payments()
            self.update_specific_payments(created_payments)

        # ensure data exists for sync streams and set expectations
        expected_records = self.create_test_data(self.TESTABLE_STREAMS, self.START_DATE, force_create_records=True)

        # instantiate connection
        conn_id = connections.ensure_connection(self, payload_hook=self.preserve_access_token)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        streams_to_select = self.testable_streams(environment, data_type)
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, streams_to_select, select_all_fields=True
        )

        # run initial sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)

        replicated_row_count = sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Verify target has records for all synced streams
        for stream, count in first_record_count_by_stream.items():
            assert stream in self.expected_streams()
            self.assertGreater(count, 0, msg="failed to replicate any data for: {}".format(stream))
        LOGGER.info('total replicated row count: {}'.format(replicated_row_count))

        MISSING_FROM_EXPECTATIONS = { # this is acceptable, we can't generate test data for EVERYTHING
            'modifier_lists': {'absent_at_location_ids'},
            'items': {'present_at_location_ids', 'absent_at_location_ids'},
            'categories': {'absent_at_location_ids'},
            'orders': {
                'amount_money', 'delayed_until', 'order_id', 'reason', 'processing_fee',
                'tax_data','status','is_deleted','discount_data','delay_duration','source_type',
                'receipt_number','receipt_url','card_details','delay_action','type','category_data',
                'payment_id','refund_ids','note','present_at_all_locations', 'refunded_money',
                'discounts', 'reference_id', 'taxes', 'pricing_options', 'service_charges'
            },
            'discounts': {'absent_at_location_ids'},
            'taxes': {'absent_at_location_ids'},
            'customers': {'birthday', 'tax_ids', 'group_ids', 'reference_id', 'version', 'segment_ids', 'phone_number'},
            'payments': {
                'customer_id', 'reference_id',
                'cash_details', 'tip_money', 'external_details', 'device_details',
                'wallet_details', 'risk_evaluation', 'statement_description_identifier',
                'buy_now_pay_later_details', 'team_member_id', 'buyer_email_address',
                'app_fee_money', 'bank_account_details', 'shipping_address', 'billing_address'
            },
            'locations': {'facebook_url', 'pos_background_url', 'full_format_logo_url', 'logo_url'},
            'refunds': {'destination_details', 'unlinked', 'team_member_id', 'app_fee_money'}
        }

        # BUG_1 | https://stitchdata.atlassian.net/browse/SRCE-4975
        PARENT_FIELD_MISSING_SUBFIELDS = {'payments': {'card_details'},
                                          'orders': {'line_items', 'returns'},
                                          'categories': {'category_data'},
                                          'discounts': {'discount_data'}}

        # BUG_2 | https://stitchdata.atlassian.net/browse/SRCE-5143
        MISSING_FROM_SCHEMA = {
            'payments': {'capabilities', 'version_token', 'approved_money',},
            'orders': {
                'line_items',
                'category_data', 'amount_money', 'processing_fee', 'refund_ids', 'delayed_until',
                'delay_duration', 'delay_action', 'note', 'status', 'order_id', 'type',
                'source_type', 'payment_id', 'tax_data', 'receipt_number', 'receipt_url',
                'discount_data', 'refunded_money', 'present_at_all_locations', 'card_details',
                'is_deleted', 'reason'},
            'discounts': {'created_at'},
            'items': {'created_at'},
            'modifier_lists': {'created_at'},
            'categories': {'created_at'},
            'taxes': {'created_at'},
            'locations': {'capabilities'}
        }

        # Test by Stream
        for stream in self.TESTABLE_STREAMS:
            with self.subTest(stream=stream):
                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = set()
                for record in expected_records.get(stream):
                    expected_keys.update(record.keys())

                # Verify schema matches expectations
                schema_keys = set(self.expected_schema_keys(stream))
                schema_keys.update(MISSING_FROM_SCHEMA.get(stream, set()))  # REMOVE W/ BUG_2 FIX
                expected_keys.update(MISSING_FROM_EXPECTATIONS.get(stream, set()))
                self.assertSetEqual(expected_keys, schema_keys)

                # Verify that all fields sent to the target fall into the expected schema
                for actual_keys in record_messages_keys:
                    self.assertTrue(
                        actual_keys.issubset(schema_keys),
                        msg="Expected all fields to be present, as defined by schemas/{}.json".format(stream) +
                        "EXPECTED (SCHEMA): {}\nACTUAL (REPLICATED KEYS): {}".format(schema_keys, actual_keys))

                actual_records = [row['data'] for row in data['messages']]

                # Verify by pks, that we replicated the expected records and only the expected records
                self.assertPKsEqual(stream, expected_records.get(stream), actual_records)

                expected_pks_to_record_dict = self.getPKsToRecordsDict(stream, expected_records.get(stream))
                actual_pks_to_record_dict = self.getPKsToRecordsDict(stream, actual_records)

                for pks_tuple, expected_record in expected_pks_to_record_dict.items():
                    actual_record = actual_pks_to_record_dict.get(pks_tuple)

                    # Test Workaround Start ##############################
                    if stream in PARENT_FIELD_MISSING_SUBFIELDS.keys():

                        off_keys = MISSING_FROM_SCHEMA[stream] # BUG_2
                        self.assertParentKeysEqualWithOffKeys(
                            expected_record, actual_record, off_keys
                        )
                        off_keys = PARENT_FIELD_MISSING_SUBFIELDS[stream] | MISSING_FROM_SCHEMA[stream] # BUG_1 | # BUG_2
                        self.assertDictEqualWithOffKeys(
                            expected_record, actual_record, off_keys
                        )

                    else:  # Test Workaround End ##############################

                        self.assertRecordsEqual(stream, expected_record, actual_record)
