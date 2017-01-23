# -*- coding: utf-8 -*-
""" Core components """
from boto.exception import JSONResponseError, BotoServerError

from dynamic_dynamodb import calculators
from dynamic_dynamodb.aws import dynamodb, sns
from dynamic_dynamodb.core import circuit_breaker
from dynamic_dynamodb.statistics import table as table_stats
from dynamic_dynamodb.log_handler import LOGGER as logger
from dynamic_dynamodb.config_handler import get_global_option

import dynamic_dynamodb.config
import json

def ensure_provisioning(
        _consulapi,
        table_name, table_key,
        num_consec_read_checks,
        num_consec_write_checks):
    """ Ensure that provisioning is correct

    :type table_name: str
    :param table_name: Name of the DynamoDB table
    :type table_key: str
    :param table_key: Configuration option key name
    :type num_consec_read_checks: int
    :param num_consec_read_checks: How many consecutive checks have we had
    :type num_consec_write_checks: int
    :param num_consec_write_checks: How many consecutive checks have we had
    :returns: (int, int) -- num_consec_read_checks, num_consec_write_checks
    """
    l_tableConfigPath = "dynamic-dynamodb/" + table_name
    CONFIGURATION = dynamic_dynamodb.config.get_configuration()
    l_tableConfig = CONFIGURATION['tables'][table_key].copy()
    del l_tableConfig["gsis"];

    try:
      l_tmp, l_data = _consulapi.kv.get(l_tableConfigPath)

      if l_data is not None:
        l_data = json.loads(l_data["Value"])
        l_tableConfig.update(l_data)

    except Exception as e:
      logger.error("{0} - Can't read config from consul for table: {1}".format(table_name, e))

    if not get_global_option('dry_run'):
      try:
        _consulapi.kv.put(l_tableConfigPath, json.dumps(l_tableConfig))

      except Exception as e:
        logger.error("{0} - Can't update config in consul for table: {1}".format(table_name, e))

    if get_global_option('circuit_breaker_url') or l_tableConfig.get('circuit_breaker_url'):
        if circuit_breaker.is_open(table_name, table_key):
            logger.warning('Circuit breaker is OPEN!')
            return (0, 0)

    try:
        read_update_needed, updated_read_units, num_consec_read_checks = \
            __ensure_provisioning_reads(
                l_tableConfig,
                table_name,
                num_consec_read_checks)
        write_update_needed, updated_write_units, num_consec_write_checks = \
            __ensure_provisioning_writes(
                l_tableConfig,
                table_name,
                num_consec_write_checks)

        if read_update_needed:
            num_consec_read_checks = 0

        if write_update_needed:
            num_consec_write_checks = 0

        # Handle throughput updates
        l_unpdate_throughput = False
        l_logline = '{0} - No need to change provisioning'

        if l_tableConfig.get('always_decrease_rw_together'):
            l_unpdate_throughput = read_update_needed and write_update_needed

            if not l_unpdate_throughput:
                if read_update_needed or write_update_needed:
                    l_logline = '{{0}} - always_decrease_rw_together is set, but read_update_needed: {0}, write_update_needed: {1}'. format(read_update_needed, write_update_needed)

        else:
            l_unpdate_throughput = read_update_needed or write_update_needed

        if l_unpdate_throughput:
            __update_throughput(
                l_tableConfig,
                table_name,
                table_key,
                updated_read_units,
                updated_write_units)

        else:
            logger.info(l_logline.format(table_name))

    except JSONResponseError:
        raise

    except BotoServerError:
        raise

    return num_consec_read_checks, num_consec_write_checks


def __ensure_provisioning_reads(_tableConfig, table_name, num_consec_read_checks):
    """ Ensure that provisioning is correct

    :type table_name: str
    :param table_name: Name of the DynamoDB table
    :type num_consec_read_checks: int
    :param num_consec_read_checks: How many consecutive checks have we had
    :returns: (bool, int, int)
        update_needed, updated_read_units, num_consec_read_checks
    """
    if not _tableConfig.get('enable_reads_autoscaling'):
        logger.info('{0} - Autoscaling of reads has been disabled'.format(table_name))
        return False, dynamodb.get_provisioned_table_read_units(table_name), 0

    update_needed = False
    try:
        lookback_window_start = _tableConfig.get('lookback_window_start')
        lookback_period = _tableConfig.get('lookback_period')
        current_read_units = dynamodb.get_provisioned_table_read_units(
            table_name)
        consumed_read_units_percent = \
            table_stats.get_consumed_read_units_percent(
                table_name, lookback_window_start, lookback_period)
        throttled_read_count = \
            table_stats.get_throttled_read_event_count(
                table_name, lookback_window_start, lookback_period)
        throttled_by_provisioned_read_percent = \
            table_stats.get_throttled_by_provisioned_read_event_percent(
                table_name, lookback_window_start, lookback_period)
        throttled_by_consumed_read_percent = \
            table_stats.get_throttled_by_consumed_read_percent(
                table_name, lookback_window_start, lookback_period)
        reads_upper_threshold = \
            _tableConfig.get('reads_upper_threshold')
        reads_lower_threshold = \
            _tableConfig.get('reads_lower_threshold')
        throttled_reads_upper_threshold = \
            _tableConfig.get('throttled_reads_upper_threshold')
        increase_reads_with = \
            _tableConfig.get('increase_reads_with')
        increase_reads_unit = \
            _tableConfig.get('increase_reads_unit')
        decrease_reads_with = \
            _tableConfig.get('decrease_reads_with')
        decrease_reads_unit = \
            _tableConfig.get('decrease_reads_unit')
        min_provisioned_reads = \
            _tableConfig.get('min_provisioned_reads')
        max_provisioned_reads = \
            _tableConfig.get('max_provisioned_reads')
        num_read_checks_before_scale_down = \
            _tableConfig.get('num_read_checks_before_scale_down')
        num_read_checks_reset_percent = \
            _tableConfig.get('num_read_checks_reset_percent')
        increase_throttled_by_provisioned_reads_unit = \
            _tableConfig.get('increase_throttled_by_provisioned_reads_unit')
        increase_throttled_by_provisioned_reads_scale = \
            _tableConfig.get('increase_throttled_by_provisioned_reads_scale')
        increase_throttled_by_consumed_reads_unit = \
            _tableConfig.get('increase_throttled_by_consumed_reads_unit')
        increase_throttled_by_consumed_reads_scale = \
            _tableConfig.get('increase_throttled_by_consumed_reads_scale')
        increase_consumed_reads_unit = \
            _tableConfig.get('increase_consumed_reads_unit')
        increase_consumed_reads_with = \
            _tableConfig.get('increase_consumed_reads_with')
        increase_consumed_reads_scale = \
            _tableConfig.get('increase_consumed_reads_scale')
        decrease_consumed_reads_unit = \
            _tableConfig.get('decrease_consumed_reads_unit')
        decrease_consumed_reads_with = \
            _tableConfig.get('decrease_consumed_reads_with')
        decrease_consumed_reads_scale = \
            _tableConfig.get('decrease_consumed_reads_scale')
    except JSONResponseError:
        raise
    except BotoServerError:
        raise

    # Set the updated units to the current read unit value
    updated_read_units = current_read_units

    # Reset consecutive reads if num_read_checks_reset_percent is reached
    if num_read_checks_reset_percent:

        if consumed_read_units_percent >= num_read_checks_reset_percent:

            logger.info(
                '{0} - Resetting the number of consecutive '
                'read checks. Reason: Consumed percent {1} is '
                'greater than reset percent: {2}'.format(
                    table_name,
                    consumed_read_units_percent,
                    num_read_checks_reset_percent))

            num_consec_read_checks = 0

    if (consumed_read_units_percent == 0 and not
            _tableConfig.get('allow_scaling_down_reads_on_0_percent')):
        logger.info('{0} - Scaling down reads is not done when usage is at 0%'.format(table_name))

    # Exit if up scaling has been disabled
    if not _tableConfig.get('enable_reads_up_scaling'):
        logger.debug(
            '{0} - Up scaling event detected. No action taken as scaling '
            'up reads has been disabled in the configuration'.format(table_name))

    else:

        # If local/granular values not specified use global values
        increase_consumed_reads_unit = \
            increase_consumed_reads_unit or increase_reads_unit
        increase_throttled_by_provisioned_reads_unit = \
            increase_throttled_by_provisioned_reads_unit or increase_reads_unit
        increase_throttled_by_consumed_reads_unit = \
            increase_throttled_by_consumed_reads_unit or increase_reads_unit

        increase_consumed_reads_with = \
            increase_consumed_reads_with or increase_reads_with

        # Initialise variables to store calculated provisioning
        throttled_by_provisioned_calculated_provisioning = scale_reader(
            increase_throttled_by_provisioned_reads_scale,
            throttled_by_provisioned_read_percent)
        throttled_by_consumed_calculated_provisioning = scale_reader(
            increase_throttled_by_consumed_reads_scale,
            throttled_by_consumed_read_percent)
        consumed_calculated_provisioning = scale_reader(
            increase_consumed_reads_scale,
            consumed_read_units_percent)
        throttled_count_calculated_provisioning = 0
        calculated_provisioning = 0

        # Increase needed due to high throttled to provisioned ratio
        if throttled_by_provisioned_calculated_provisioning:

            throttled_by_provisioned_calculated_provisioning = calculators.increase_reads(
              increase_throttled_by_provisioned_reads_unit,
              current_read_units,
              throttled_by_provisioned_calculated_provisioning,
              _tableConfig.get('max_provisioned_reads'),
              consumed_read_units_percent,
              table_name
            )

        # Increase needed due to high throttled to consumed ratio
        if throttled_by_consumed_calculated_provisioning:

            throttled_by_consumed_calculated_provisioning = calculators.increase_reads(
              increase_throttled_by_consumed_reads_unit,
              current_read_units,
              throttled_by_consumed_calculated_provisioning,
              _tableConfig.get('max_provisioned_reads'),
              consumed_read_units_percent,
              table_name
            )

        # Increase needed due to high CU consumption
        if consumed_calculated_provisioning:

            consumed_calculated_provisioning = calculators.increase_reads(
              increase_consumed_reads_unit,
              current_read_units,
              consumed_calculated_provisioning,
              _tableConfig.get('max_provisioned_reads'),
              consumed_read_units_percent,
              table_name)

        elif reads_upper_threshold and consumed_read_units_percent > reads_upper_threshold:

            consumed_calculated_provisioning =  calculators.increase_reads(
              increase_consumed_reads_unit,
              current_read_units,
              increase_consumed_reads_with,
              _tableConfig.get('max_provisioned_reads'),
              consumed_read_units_percent,
              table_name
            )

        # Increase needed due to high throttling
        if throttled_reads_upper_threshold and throttled_read_count > throttled_reads_upper_threshold:

            throttled_count_calculated_provisioning = calculators.increase_reads_in_percent(
                increase_reads_unit,
                updated_read_units,
                increase_consumed_reads_with,
                _tableConfig.get('max_provisioned_reads'),
                consumed_read_units_percent,
                table_name
            )

        # Determine which metric requires the most scaling
        if (throttled_by_provisioned_calculated_provisioning
                > calculated_provisioning):
            calculated_provisioning = \
                throttled_by_provisioned_calculated_provisioning
            scale_reason = (
                "due to throttled events by provisioned "
                "units threshold being exceeded")
        if (throttled_by_consumed_calculated_provisioning
                > calculated_provisioning):
            calculated_provisioning = \
                throttled_by_consumed_calculated_provisioning
            scale_reason = (
                "due to throttled events by consumed "
                "units threshold being exceeded")
        if consumed_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = consumed_calculated_provisioning
            scale_reason = "due to consumed threshold being exceeded"
        if throttled_count_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = throttled_count_calculated_provisioning
            scale_reason = "due to throttled events threshold being exceeded"

        if calculated_provisioning > current_read_units:
            logger.info(
                '{0} - Resetting the number of consecutive '
                'read checks. Reason: scale up {1}'.format(
                    table_name, scale_reason))
            num_consec_read_checks = 0
            update_needed = True
            updated_read_units = calculated_provisioning

    # Decrease needed due to low CU consumption
    if not update_needed:
        # If local/granular values not specified use global values
        decrease_consumed_reads_unit = \
            decrease_consumed_reads_unit or decrease_reads_unit

        decrease_consumed_reads_with = \
            decrease_consumed_reads_with or decrease_reads_with

        # Exit if down scaling has been disabled
        if not _tableConfig.get('enable_reads_down_scaling'):
            logger.debug(
                '{0} - Down scaling event detected. No action taken as scaling'
                ' down reads has been disabled in the configuration'.format(
                    table_name))
        else:
            # Initialise variables to store calculated provisioning
            calculated_provisioning = None
            consumed_calculated_provisioning = scale_reader_decrease(
              decrease_consumed_reads_scale,
              consumed_read_units_percent)

            if consumed_calculated_provisioning:
                calculated_provisioning = calculators.decrease_reads(
                  decrease_consumed_reads_unit,
                  updated_read_units,
                  consumed_calculated_provisioning,
                  _tableConfig.get('min_provisioned_reads'),
                  table_name
                )

            elif reads_lower_threshold and consumed_read_units_percent < reads_lower_threshold:
                calculated_provisioning = calculators.decrease_reads(
                  decrease_consumed_reads_unit,
                  updated_read_units,
                  decrease_consumed_reads_with,
                  _tableConfig.get('min_provisioned_reads'),
                  table_name
                )

            if calculated_provisioning and current_read_units != calculated_provisioning:
                num_consec_read_checks += 1

                if num_consec_read_checks >= num_read_checks_before_scale_down:
                    update_needed = True
                    updated_read_units = calculated_provisioning

            logger.debug('{0} - Consecutive read checks {1}/{2}'.format(
                table_name,
                num_consec_read_checks,
                num_read_checks_before_scale_down))

    if calculators.is_consumed_over_proposed(
            current_read_units,
            updated_read_units,
            consumed_read_units_percent):
        update_needed = False
        updated_read_units = current_read_units
        logger.info(
            '{0} - Consumed is over proposed read units. Will leave table at '
            'current setting.'.format(table_name))

    return update_needed, updated_read_units, num_consec_read_checks


def __ensure_provisioning_writes(_tableConfig, table_name, num_consec_write_checks):
    """ Ensure that provisioning of writes is correct

    :type table_name: str
    :param table_name: Name of the DynamoDB table
    :type num_consec_write_checks: int
    :param num_consec_write_checks: How many consecutive checks have we had
    :returns: (bool, int, int)
        update_needed, updated_write_units, num_consec_write_checks
    """
    if not _tableConfig.get('enable_writes_autoscaling'):
        logger.info(
            '{0} - Autoscaling of writes has been disabled'.format(table_name))
        return False, dynamodb.get_provisioned_table_write_units(table_name), 0

    update_needed = False
    try:
        lookback_window_start = _tableConfig.get('lookback_window_start')
        lookback_period = _tableConfig.get('lookback_period')
        provisioned_write_units = dynamodb.get_provisioned_table_write_units(table_name)

        consumed_write_units_percent = \
            table_stats.get_consumed_write_units_percent(
                table_name, lookback_window_start, lookback_period)
        throttled_write_count = \
            table_stats.get_throttled_write_event_count(
                table_name, lookback_window_start, lookback_period)
        throttled_by_provisioned_write_percent = \
            table_stats.get_throttled_by_provisioned_write_event_percent(
                table_name, lookback_window_start, lookback_period)
        throttled_by_consumed_write_percent = \
            table_stats.get_throttled_by_consumed_write_percent(
                table_name, lookback_window_start, lookback_period)
        writes_upper_threshold = \
            _tableConfig.get('writes_upper_threshold')
        writes_lower_threshold = \
            _tableConfig.get('writes_lower_threshold')
        throttled_writes_upper_threshold = \
            _tableConfig.get('throttled_writes_upper_threshold')
        increase_writes_unit = \
            _tableConfig.get('increase_writes_unit')
        increase_writes_with = \
            _tableConfig.get('increase_writes_with')
        decrease_writes_unit = \
            _tableConfig.get('decrease_writes_unit')
        decrease_writes_with = \
            _tableConfig.get('decrease_writes_with')
        min_provisioned_writes = \
            _tableConfig.get('min_provisioned_writes')

        max_provisioned_writes = _tableConfig.get('max_provisioned_writes')
        max_provisioned_writes = int(max_provisioned_writes) if max_provisioned_writes else None

        num_write_checks_before_scale_down = \
            _tableConfig.get('num_write_checks_before_scale_down')
        num_write_checks_reset_percent = \
            _tableConfig.get('num_write_checks_reset_percent')
        increase_throttled_by_provisioned_writes_unit = \
            _tableConfig.get('increase_throttled_by_provisioned_writes_unit')
        increase_throttled_by_provisioned_writes_scale = \
            _tableConfig.get('increase_throttled_by_provisioned_writes_scale')
        increase_throttled_by_consumed_writes_unit = \
            _tableConfig.get('increase_throttled_by_consumed_writes_unit')
        increase_throttled_by_consumed_writes_scale = \
            _tableConfig.get('increase_throttled_by_consumed_writes_scale')
        increase_consumed_writes_unit = \
            _tableConfig.get('increase_consumed_writes_unit')
        increase_consumed_writes_with = \
            _tableConfig.get('increase_consumed_writes_with')
        increase_consumed_writes_scale = \
            _tableConfig.get('increase_consumed_writes_scale')
        decrease_consumed_writes_unit = \
            _tableConfig.get('decrease_consumed_writes_unit')
        decrease_consumed_writes_with = \
            _tableConfig.get('decrease_consumed_writes_with')
        decrease_consumed_writes_scale = \
            _tableConfig.get('decrease_consumed_writes_scale')
    except JSONResponseError:
        raise
    except BotoServerError:
        raise

    # Set the updated units to the current write unit value
    updated_write_units = provisioned_write_units

    # Reset consecutive write count if num_write_checks_reset_percent
    # is reached
    if num_write_checks_reset_percent:

        if consumed_write_units_percent >= num_write_checks_reset_percent:

            logger.info(
                '{0} - Resetting the number of consecutive '
                'write checks. Reason: Consumed percent {1} is '
                'greater than reset percent: {2}'.format(
                    table_name,
                    consumed_write_units_percent,
                    num_write_checks_reset_percent))

            num_consec_write_checks = 0

    # Check if we should update write provisioning
    if consumed_write_units_percent == 0 and not _tableConfig.get('allow_scaling_down_writes_on_0_percent'):
        logger.info('{0} - Scaling down writes is not done when usage is at 0%'.format(table_name))

    # Exit if up scaling has been disabled
    if not _tableConfig.get('enable_writes_up_scaling'):
        logger.debug(
            '{0} - Up scaling event detected. No action taken as scaling '
            'up writes has been disabled in the configuration'.format(table_name))

    else:

        # If local/granular values not specified use global values
        increase_consumed_writes_unit = \
            increase_consumed_writes_unit or increase_writes_unit
        increase_throttled_by_provisioned_writes_unit = (
            increase_throttled_by_provisioned_writes_unit
            or increase_writes_unit)
        increase_throttled_by_consumed_writes_unit = \
            increase_throttled_by_consumed_writes_unit or increase_writes_unit

        increase_consumed_writes_with = \
            increase_consumed_writes_with or increase_writes_with

        # Initialise variables to store calculated provisioning
        throttled_by_provisioned_calculated_provisioning = scale_reader(
            increase_throttled_by_provisioned_writes_scale,
            throttled_by_provisioned_write_percent)
        throttled_by_consumed_calculated_provisioning = scale_reader(
            increase_throttled_by_consumed_writes_scale,
            throttled_by_consumed_write_percent)
        consumed_calculated_provisioning = scale_reader(
            increase_consumed_writes_scale, consumed_write_units_percent)
        throttled_count_calculated_provisioning = 0
        calculated_provisioning = 0

        # Increase needed due to high throttled to provisioned ratio
        if throttled_by_provisioned_calculated_provisioning:

            throttled_by_provisioned_calculated_provisioning = calculators.increase_writes(
              increase_throttled_by_provisioned_writes_unit,
              provisioned_write_units,
              throttled_by_provisioned_calculated_provisioning,
              max_provisioned_writes,
              consumed_write_units_percent,
              table_name
            )

        # Increase needed due to high throttled to consumed ratio
        if throttled_by_consumed_calculated_provisioning:

            throttled_by_consumed_calculated_provisioning = calculators.increase_writes(
              increase_throttled_by_consumed_writes_unit,
              provisioned_write_units,
              throttled_by_consumed_calculated_provisioning,
              max_provisioned_writes,
              consumed_write_units_percent,
              table_name
            )

        # Increase needed due to high CU consumption
        if consumed_calculated_provisioning:

            consumed_calculated_provisioning = calculators.increase_writes(
              increase_consumed_writes_unit,
              provisioned_write_units,
              consumed_calculated_provisioning,
              max_provisioned_writes,
              consumed_write_units_percent,
              table_name
            )

        elif writes_upper_threshold and consumed_write_units_percent > writes_upper_threshold:

            consumed_calculated_provisioning = calculators.increase_writes(
              increase_consumed_writes_unit,
              provisioned_write_units,
              increase_consumed_writes_with,
              max_provisioned_writes,
              consumed_write_units_percent,
              table_name
            )

        # Increase needed due to high throttling
        if (throttled_writes_upper_threshold and throttled_write_count >
                throttled_writes_upper_threshold):

            throttled_count_calculated_provisioning = calculators.increase_writes(
              increase_writes_unit, 
              provisioned_write_units,
              increase_writes_with,
              max_provisioned_writes,
              consumed_write_units_percent,
              table_name
            )

        # Determine which metric requires the most scaling
        if throttled_by_provisioned_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = throttled_by_provisioned_calculated_provisioning
            scale_reason = (
                "due to throttled events by provisioned "
                "units threshold being exceeded")

        if throttled_by_consumed_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = throttled_by_consumed_calculated_provisioning
            scale_reason = (
                "due to throttled events by consumed "
                "units threshold being exceeded")

        if consumed_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = consumed_calculated_provisioning
            scale_reason = "due to consumed threshold being exceeded"

        if throttled_count_calculated_provisioning > calculated_provisioning:
            calculated_provisioning = throttled_count_calculated_provisioning
            scale_reason = "due to throttled events threshold being exceeded"

        if calculated_provisioning > provisioned_write_units:
            logger.info(
                '{0} - Resetting the number of consecutive '
                'write checks. Reason: scale up {1}'.format(
                    table_name, scale_reason))
            num_consec_write_checks = 0
            update_needed = True
            updated_write_units = calculated_provisioning

    # Decrease needed due to low CU consumption
    if not update_needed:
        # If local/granular values not specified use global values
        decrease_consumed_writes_unit = \
            decrease_consumed_writes_unit or decrease_writes_unit

        decrease_consumed_writes_with = \
            decrease_consumed_writes_with or decrease_writes_with

        # Initialise variables to store calculated provisioning
        consumed_calculated_provisioning = scale_reader_decrease(
            decrease_consumed_writes_scale,
            consumed_write_units_percent)
        calculated_provisioning = None

        # Exit if down scaling has been disabled
        if not _tableConfig.get('enable_writes_down_scaling'):
            logger.debug(
                '{0} - Down scaling event detected. No action taken as scaling'
                ' down writes has been disabled in the configuration'.format(
                    table_name))
        else:
            if consumed_calculated_provisioning:
                calculated_provisioning = calculators.decrease_writes(
                  decrease_consumed_writes_unit,
                  provisioned_write_units,
                  consumed_calculated_provisioning,
                  _tableConfig.get('min_provisioned_writes'),
                  table_name
                )

            elif writes_lower_threshold and consumed_write_units_percent < writes_lower_threshold:
                calculated_provisioning = calculators.decrease_writes(
                  decrease_consumed_writes_unit,
                  provisioned_write_units,
                  decrease_consumed_writes_with,
                  _tableConfig.get('min_provisioned_writes'),
                  table_name
                )

            if (calculated_provisioning and
                    provisioned_write_units != calculated_provisioning):
                num_consec_write_checks += 1

                if num_consec_write_checks >= \
                        num_write_checks_before_scale_down:
                    update_needed = True
                    updated_write_units = calculated_provisioning

            logger.debug('{0} - Consecutive write checks {1}/{2}'.format(
                table_name,
                num_consec_write_checks,
                num_write_checks_before_scale_down))

    if calculators.is_consumed_over_proposed(
            provisioned_write_units,
            updated_write_units,
            consumed_write_units_percent):
        update_needed = False
        updated_write_units = provisioned_write_units
        logger.debug(
            '{0} - Consumed is over proposed write units. Will leave table at '
            'current setting.'.format(table_name))

    return update_needed, updated_write_units, num_consec_write_checks


def __update_throughput(_tableConfig, table_name, key_name, read_units, write_units):
    """ Update throughput on the DynamoDB table

    :type table_name: str
    :param table_name: Name of the DynamoDB table
    :type key_name: str
    :param key_name: Configuration option key name
    :type read_units: int
    :param read_units: New read unit provisioning
    :type write_units: int
    :param write_units: New write unit provisioning
    """
    
    # Check table status
    try:
        table_status = dynamodb.get_table_status(table_name)
    except JSONResponseError:
        raise
    logger.debug('{0} - Table status is {1}'.format(table_name, table_status))
    if table_status != 'ACTIVE':
        logger.warning(
            '{0} - Not performing throughput changes when table '
            'is {1}'.format(table_name, table_status))
        return

    logger.info(
        '{0} - Changing provisioning to {1:d} '
        'read units and {2:d} write units'.format(
            table_name,
            int(read_units),
            int(write_units)))

    dynamodb.update_table_provisioning(
        table_name,
        key_name,
        int(read_units),
        int(write_units))

def scale_reader(provision_increase_scale, current_value):
    """

    :type provision_increase_scale: dict
    :param provision_increase_scale: dictionary with key being the
        scaling threshold and value being scaling amount
    :type current_value: float
    :param current_value: the current consumed units or throttled events
    :returns: (int) The amount to scale provisioning by
    """

    scale_value = 0
    if provision_increase_scale:
        for limits in sorted(provision_increase_scale.keys()):
            if current_value < limits:
                return scale_value
            else:
                scale_value = provision_increase_scale.get(limits)
        return scale_value
    else:
        return scale_value


def scale_reader_decrease(provision_decrease_scale, current_value):
    """

    :type provision_decrease_scale: dict
    :param provision_decrease_scale: dictionary with key being the
        scaling threshold and value being scaling amount
    :type current_value: float
    :param current_value: the current consumed units or throttled events
    :returns: (int) The amount to scale provisioning by
    """
    scale_value = 0
    if provision_decrease_scale:
        for limits in sorted(provision_decrease_scale.keys(), reverse=True):
            if current_value > limits:
                return scale_value
            else:
                scale_value = provision_decrease_scale.get(limits)
        return scale_value
    else:
        return scale_value
