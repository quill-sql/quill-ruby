module StringOperator
  IS_EXACTLY = 'is exactly'
  IS_NOT_EXACTLY = 'is not exactly'
  CONTAINS = 'contains'
  IS = 'is'
  IS_NOT = 'is not'
end

module DateOperator
  CUSTOM = 'custom'
  IN_THE_LAST = 'in the last'
  IN_THE_PREVIOUS = 'in the previous'
  IN_THE_CURRENT = 'in the current'
  EQUAL_TO = 'equal to'
  NOT_EQUAL_TO = 'not equal to'
  GREATER_THAN = 'greater than'
  LESS_THAN = 'less than'
  GREATER_THAN_OR_EQUAL_TO = 'greater than or equal to'
  LESS_THAN_OR_EQUAL_TO = 'less than or equal to'
end

module NumberOperator
  EQUAL_TO = 'equal to'
  NOT_EQUAL_TO = 'not equal to'
  GREATER_THAN = 'greater than'
  LESS_THAN = 'less than'
  GREATER_THAN_OR_EQUAL_TO = 'greater than or equal to'
  LESS_THAN_OR_EQUAL_TO = 'less than or equal to'
end

module NullOperator
  IS_NOT_NULL = 'is not null'
  IS_NULL = 'is null'
end

module BoolOperator
  EQUAL_TO = 'equal to'
  NOT_EQUAL_TO = 'not equal to'
end

module TimeUnit
  YEAR = 'year'
  QUARTER = 'quarter'
  MONTH = 'month'
  WEEK = 'week'
  DAY = 'day'
  HOUR = 'hour'
end

module FieldType
  STRING = 'string'
  NUMBER = 'number'
  DATE = 'date'
  NULL = 'null'
  BOOLEAN = 'boolean'
end

module FilterType
  STRING_FILTER = 'string-filter'
  DATE_FILTER = 'date-filter'
  DATE_CUSTOM_FILTER = 'date-custom-filter'
  DATE_COMPARISON_FILTER = 'date-comparison-filter'
  NUMERIC_FILTER = 'numeric-filter'
  NULL_FILTER = 'null-filter'
  STRING_IN_FILTER = 'string-in-filter'
  BOOLEAN_FILTER = 'boolean-filter'
end

module FilterUtils
  def convert_custom_filter(filter)
    case filter[:filter_type]
    when FilterType::STRING_FILTER
      validate_string_filter(filter)
      filter.merge(field_type: FieldType::STRING)
    when FilterType::STRING_IN_FILTER
      validate_string_in_filter(filter)
      filter.merge(field_type: FieldType::STRING)
    when FilterType::NUMERIC_FILTER
      validate_numeric_filter(filter)
      filter.merge(field_type: FieldType::NUMBER)
    when FilterType::NULL_FILTER
      validate_null_filter(filter)
      filter.merge(field_type: FieldType::NULL)
    when FilterType::BOOLEAN_FILTER
      validate_boolean_filter(filter)
      filter.merge(field_type: FieldType::BOOLEAN)
    when FilterType::DATE_FILTER
      validate_date_filter(filter)
      filter.merge(field_type: FieldType::DATE)
    when FilterType::DATE_CUSTOM_FILTER
      validate_date_custom_filter(filter)
      filter.merge(field_type: FieldType::DATE)
    when FilterType::DATE_COMPARISON_FILTER
      validate_date_comparison_filter(filter)
      filter.merge(field_type: FieldType::DATE)
    end
  end
end

private

def validate_string_filter(filter)
  raise "Invalid value for StringFilter" unless filter[:value].is_a?(String)
  raise "Invalid operator for StringFilter" unless StringOperator.constants.map { |c| StringOperator.const_get(c) }.include?(filter[:operator])
end

def validate_string_in_filter(filter)
  raise "Invalid value for StringInFilter" unless filter[:value].is_a?(Array)
  raise "Invalid operator for StringInFilter" unless StringOperator.constants.map { |c| StringOperator.const_get(c) }.include?(filter[:operator])
end

def validate_numeric_filter(filter)
  raise "Invalid value for NumericFilter" unless filter[:value].is_a?(Numeric)
  raise "Invalid operator for NumericFilter" unless NumberOperator.constants.map { |c| NumberOperator.const_get(c) }.include?(filter[:operator])
end

def validate_null_filter(filter)
  raise "Invalid value for NullFilter" unless filter[:value].nil?
  raise "Invalid operator for NullFilter" unless NullOperator.constants.map { |c| NullOperator.const_get(c) }.include?(filter[:operator])
end

def validate_boolean_filter(filter)
  raise "Invalid value for BooleanFilter" unless [true, false].include?(filter[:value])
  raise "Invalid operator for BooleanFilter" unless BoolOperator.constants.map { |c| BoolOperator.const_get(c) }.include?(filter[:operator])
end

def validate_date_filter(filter)
  value = filter[:value]
  raise "Invalid value for DateFilter" unless value.is_a?(Hash) && value[:value].is_a?(Numeric) && value[:unit].is_a?(String)
  raise "Invalid operator for DateFilter" unless DateOperator.constants.map { |c| DateOperator.const_get(c) }.include?(filter[:operator])
end

def validate_date_custom_filter(filter)
  value = filter[:value]
  raise "Invalid value for DateCustomFilter" unless value.is_a?(Hash) && value[:start_date].is_a?(String) && value[:end_date].is_a?(String)
  raise "Invalid operator for DateCustomFilter" unless DateOperator.constants.map { |c| DateOperator.const_get(c) }.include?(filter[:operator])
end

def validate_date_comparison_filter(filter)
  raise "Invalid value for DateComparisonFilter" unless filter[:value].is_a?(String)
  raise "Invalid operator for DateComparisonFilter" unless DateOperator.constants.map { |c| DateOperator.const_get(c) }.include?(filter[:operator])
end