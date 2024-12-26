require 'json'
require 'uri'
require_relative 'clickhouse'

module DatabaseHelper
  SUPPORTED_DATABASES = ['clickhouse'].freeze  # Add others as they're implemented

  class QuillQueryResults
    attr_reader :fields, :rows

    def initialize(fields, rows)
      @fields = fields
      @rows = rows
    end
  end

  class DatabaseError < StandardError; end

  def self.get_database_credentials(database_type, connection_string)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.format_clickhouse_config(connection_string)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.connect_to_database(database_type, config)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.connect_to_clickhouse(config)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.with_connection(database_type, connection_string)
    config = get_database_credentials(database_type, connection_string)
    connection = connect_to_database(database_type, config)
    begin
      yield(connection)
    rescue StandardError => e
      { success: false, message: e.message || e.error || e.to_s }
    ensure
      disconnect_from_database(database_type, connection)
    end
  end

  def self.run_query_by_database(database_type, connection, sql)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.run_query_clickhouse(sql, connection)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.connect_and_run_query(database_type, connection_string, sql)
    with_connection(database_type, connection_string) do |connection|
      run_query_by_database(database_type, connection, sql)
    end
  end

  def self.disconnect_from_database(database_type, database)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.disconnect_from_clickhouse(database)
    end
  end

  def self.get_schemas_by_database(database_type, connection)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.get_schemas_clickhouse(connection)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.get_tables_by_schema_by_database(database_type, connection, schema_name)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.get_tables_by_schema_clickhouse(connection, schema_name)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.get_columns_by_table_by_database(database_type, connection, schema_name, table_name)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.get_columns_by_table_clickhouse(connection, schema_name, table_name)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.get_foreign_keys_by_database(database_type, connection, schema_name, table_name, primary_key)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.get_foreign_keys_clickhouse(connection, schema_name, table_name, primary_key)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end

  def self.get_column_info_by_schema_by_database(database_type, connection, schema_name, tables)
    case database_type.downcase
    when 'clickhouse'
      ClickHouseHelper.get_schema_column_info_clickhouse(connection, schema_name, tables)
    else
      raise DatabaseError, "Invalid database type: #{database_type}"
    end
  end
end