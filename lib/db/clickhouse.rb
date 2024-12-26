require 'uri'
require 'json'
require 'click_house'

module ClickHouseHelper
  # Constants
  CLICKHOUSE_PG_TYPE_MAP = {
    # Signed Integer Types and Aliases
    'Int8' => 21,
    'TINYINT' => 21,
    'INT1' => 21,
    'BYTE' => 21,
    'TINYINT SIGNED' => 21,
    'INT1 SIGNED' => 21,
    'Int16' => 21,
    'SMALLINT' => 21,
    'SMALLINT SIGNED' => 21,
    'Int32' => 23,
    'INT' => 23,
    'INTEGER' => 23,
    'MEDIUMINT' => 23,
    'MEDIUMINT SIGNED' => 23,
    'INT SIGNED' => 23,
    'INTEGER SIGNED' => 23,
    'Int64' => 20,
    'BIGINT' => 20,
    'SIGNED' => 20,
    'BIGINT SIGNED' => 20,
    'TIME' => 20,
    # Unsigned Integer Types and Aliases
    'UInt8' => 21,
    'TINYINT UNSIGNED' => 21,
    'INT1 UNSIGNED' => 21,
    'UInt16' => 21,
    'SMALLINT UNSIGNED' => 21,
    'UInt32' => 23,
    'MEDIUMINT UNSIGNED' => 23,
    'INT UNSIGNED' => 23,
    'INTEGER UNSIGNED' => 23,
    'UInt64' => 20,
    'UNSIGNED' => 20,
    'BIGINT UNSIGNED' => 20,
    'BIT' => 20,
    'SET' => 20,
    # Floating Point Types and Aliases
    'Float32' => 700,
    'FLOAT' => 700,
    'REAL' => 700,
    'SINGLE' => 700,
    'Float64' => 701,
    'DOUBLE' => 701,
    'DOUBLE PRECISION' => 701,
    'BFloat16' => 700,
    # Decimal Types
    'Decimal' => 1700,
    'Decimal32' => 1700,
    'Decimal64' => 1700,
    'Decimal128' => 1700,
    'Decimal256' => 1700,
    # Boolean Type
    'Bool' => 16,
    # String Types and Aliases
    'String' => 25,
    'LONGTEXT' => 25,
    'MEDIUMTEXT' => 25,
    'TINYTEXT' => 25,
    'TEXT' => 25,
    'LONGBLOB' => 17,
    'MEDIUMBLOB' => 17,
    'TINYBLOB' => 17,
    'BLOB' => 17,
    'VARCHAR' => 1043,
    'CHAR' => 1042,
    'CHAR LARGE OBJECT' => 25,
    'CHAR VARYING' => 1043,
    'CHARACTER LARGE OBJECT' => 25,
    'CHARACTER VARYING' => 1043,
    'NCHAR LARGE OBJECT' => 25,
    'NCHAR VARYING' => 1043,
    'NATIONAL CHARACTER LARGE OBJECT' => 25,
    'NATIONAL CHARACTER VARYING' => 1043,
    'NATIONAL CHAR VARYING' => 1043,
    'NATIONAL CHARACTER' => 1042,
    'NATIONAL CHAR' => 1042,
    'BINARY LARGE OBJECT' => 17,
    'BINARY VARYING' => 17,
    # Fixed String
    'FixedString' => 1042,
    # Identifier Types
    'UUID' => 2950,
    # Date and Time Types
    'Date' => 1082,
    'Date32' => 1082,
    'DateTime' => 1184,
    'DateTime64' => 1184,
    # Array Types
    'Array' => 2277,
    # JSON-like Types
    'JSON' => 3802,
    'Nested' => 3802,
    # Binary Types
    'IPv4' => 17,
    'IPv6' => 17,
    # Enum Types
    'Enum8' => 10045,
    'Enum16' => 10045,
    # Geospatial-like Types
    'Point' => 17,
    'Ring' => 17,
    'Polygon' => 17,
    # Specialized Types
    'Nothing' => 17,
    'Interval' => 1186
  }.freeze

  class << self
    def parse_clickhouse_type(type)
      # Remove whitespace and handle common variations
      normalized_type = type.strip.gsub(/\s+/, ' ')

      # Handle Object types
      return CLICKHOUSE_PG_TYPE_MAP['JSON'] if normalized_type.start_with?('Map(')
      return CLICKHOUSE_PG_TYPE_MAP['JSON'] if normalized_type.start_with?('AggregateFunction(')
      return CLICKHOUSE_PG_TYPE_MAP['JSON'] if normalized_type.start_with?('SimpleAggregateFunction(')

      # Handle Nullable types
      if normalized_type.start_with?('Nullable(')
        inner_type = normalized_type[9..-2]
        return parse_clickhouse_type(inner_type)
      end

      # Handle Array types
      return CLICKHOUSE_PG_TYPE_MAP['Array'] if normalized_type.start_with?('Array(')
      return CLICKHOUSE_PG_TYPE_MAP['Array'] if normalized_type.start_with?('Tuple(')

      # Handle Enum types
      return CLICKHOUSE_PG_TYPE_MAP['Enum8'] if normalized_type.start_with?('Enum8(')
      return CLICKHOUSE_PG_TYPE_MAP['Enum16'] if normalized_type.start_with?('Enum16(')

      # Handle Decimal types
      if normalized_type.match?(/^Decimal(\d*)?\(/)
        return CLICKHOUSE_PG_TYPE_MAP['Decimal']
      end

      # Handle DateTime types
      return CLICKHOUSE_PG_TYPE_MAP['DateTime'] if normalized_type.start_with?('DateTime(')
      return CLICKHOUSE_PG_TYPE_MAP['DateTime64'] if normalized_type.start_with?('DateTime64(')

      # Handle FixedString
      return CLICKHOUSE_PG_TYPE_MAP['FixedString'] if normalized_type.start_with?('FixedString(')

      # Handle LowCardinality
      if normalized_type.start_with?('LowCardinality(')
        inner_type = normalized_type[15..-2]
        return parse_clickhouse_type(inner_type)
      end

      # Direct lookup (case-insensitive)
      lookup_type = CLICKHOUSE_PG_TYPE_MAP.keys.find { |key| key.downcase == normalized_type.downcase }
      return CLICKHOUSE_PG_TYPE_MAP[lookup_type] if lookup_type

      warn "Unknown ClickHouse type: #{type}. Defaulting to VARCHAR."
      1043 # Default to Varchar
    end

    def connect_to_clickhouse(config)
      # Configure ClickHouse connection with provided config
      ClickHouse.config do |config_object|
        config_object.url = config[:url]
        config_object.username = config[:username]
        config_object.password = config[:password]
      end
    
      # Return the connection object
      ClickHouse.connection
    end

    def disconnect_from_clickhouse(client)
      client.close if client.respond_to?(:close)
    end

    def run_query_clickhouse(sql, client)
      # Need to include FORMAT JSON at the end of query to include clickhouse metadata
      # Remove existing FORMAT from query, remove ending semicolon, and add FORMAT JSON
      response = response = client.execute(sql.gsub(/\s*FORMAT\s+\w+/i, '').gsub(/;\s*$/, '') + ' FORMAT JSON')
    
      data = response.body
  
      fields = data['meta']&.map do |field|
        {
          name: field['name'],
          dataTypeID: parse_clickhouse_type(field['type'])
        }
      end || []
    
      {
        fields: fields,
        rows: data['data']
      }
    end

    def get_schemas_clickhouse(client)
      sql = <<~SQL
        SELECT DISTINCT database AS schema_name 
        FROM system.tables 
        WHERE LOWER(database) NOT IN ('system', 'information_schema')
      SQL
      
      results = run_query_clickhouse(sql, client)
      results[:rows].map { |row| row['schema_name'] }
    end

    def get_tables_by_schema_clickhouse(client, schema_names)
      all_tables = schema_names.flat_map do |schema|
        sql = <<~SQL
          SELECT name as table_name, database as table_schema 
          FROM system.tables 
          WHERE database = '#{schema}'
        SQL
        
        results = run_query_clickhouse(sql, client)
        results[:rows].map do |row|
          {
            tableName: row['table_name'],
            schemaName: row['table_schema']
          }
        end
      end

      all_tables
    end

    def get_columns_by_table_clickhouse(client, schema_name, table_name)
      sql = <<~SQL
        SELECT name as column_name 
        FROM system.columns 
        WHERE database = '#{schema_name}' AND table = '#{table_name}'
      SQL
      
      results = run_query_clickhouse(sql, client)
      results[:rows].map { |row| row['column_name'] }
    end

    def get_foreign_keys_clickhouse(client, schema_name, table_name, primary_key)
      depluralized_table_name = depluralize(table_name)
      
      sql = <<~SQL
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '#{schema_name}' 
        AND table_name != '#{table_name}' 
        AND (column_name = '#{primary_key}' 
          OR column_name = '#{depluralized_table_name}_#{primary_key}' 
          OR column_name = '#{depluralized_table_name}#{capitalize(primary_key)}')
      SQL

      results = run_query_clickhouse(sql, client)
      foreign_keys = results[:rows].map { |key| key['column_name'] }
      
      foreign_keys = foreign_keys.reject { |key| ['id', '_id_'].include?(key) }
      foreign_keys = foreign_keys.uniq

      if foreign_keys.empty?
        sql = <<~SQL
          SELECT column_name 
          FROM information_schema.columns 
          WHERE table_schema = '#{schema_name}' 
          AND table_name != '#{table_name}' 
          AND (column_name LIKE '#{table_name}%' 
            OR column_name LIKE '%\_id' 
            OR column_name LIKE '%Id' 
            OR column_name LIKE '%\_#{primary_key}' 
            OR column_name LIKE '%#{capitalize(primary_key)}')
        SQL

        results = run_query_clickhouse(sql, client)
        foreign_keys = results[:rows].map { |key| key['column_name'] }.uniq
      end

      foreign_keys
    end

    def get_schema_column_info_clickhouse(client, schema_name, table_names)
      table_names.map do |table_name|
        query = <<~SQL
          SELECT 
            name as "column_name",
            type as "field_type"
          FROM system.columns
          WHERE database = '#{table_name[:schemaName]}'
          AND table = '#{table_name[:tableName]}'
        SQL

        results = run_query_clickhouse(query, client)
        {
          tableName: "#{table_name[:schemaName]}.#{table_name[:tableName]}",
          displayName: "#{table_name[:schemaName]}.#{table_name[:tableName]}",
          columns: results[:rows].map do |row|
            type_oid = parse_clickhouse_type(row['field_type'])
            {
              columnName: row['column_name'],
              displayName: row['column_name'],
              dataTypeID: type_oid,
              fieldType: row['field_type']
            }
          end
        }
      end
    end

    def format_clickhouse_config(connection_string)
      parsed = URI.parse(connection_string)
      
      {
        url: "#{parsed.scheme}://#{parsed.host}:#{parsed.port}",
        username: parsed.user || 'default',
        password: parsed.password || ''
      }
    end

    private

    def capitalize(str)
      str.capitalize
    end

    def depluralize(str)
      # Simple depluralization - you might want to use a proper library like ActiveSupport
      return str[0..-2] if str.end_with?('s')
      str
    end
  end
end