# Ruby 2.3 Compatible Implementation
require 'json'
require 'net/http'
require 'uri'
require 'dotenv'
require_relative './db/cached_connection'
require_relative './db/db_helper'
require_relative 'utils/tenants'
require_relative 'models/filters'
require_relative 'utils/schema'
require_relative 'utils/run_query_processes'

Dotenv.load(File.expand_path('../.env', __dir__))

module DatabaseType
  POSTGRESQL = 'postgresql'.freeze
  SNOWFLAKE = 'snowflake'.freeze
  BIGQUERY = 'bigquery'.freeze
  MYSQL = 'mysql'.freeze
  CLICKHOUSE = 'clickhouse'.freeze

  def self.valid?(type)
    constants.map { |c| const_get(c) }.include?(type)
  end
end

SINGLE_TENANT = "QUILL_SINGLE_TENANT".freeze
ALL_TENANTS = "QUILL_ALL_TENANTS".freeze

HOST = (ENV['ENV'] == 'development' ? 
    'http://localhost:8080' : 
    'https://quill-344421.uc.r.appspot.com').freeze

FLAG_TASKS = Set.new(['dashboard', 'report', 'item', 'report-info']).freeze

class QuillAPIError < StandardError; end

class Quill
  attr_reader :target_connection, :base_url, :config

  def initialize(options = {})
    validate_options(options)    
    @base_url = options[:metadata_server_url] || HOST
    @config = { headers: { 'Authorization' => "Bearer #{options[:private_key]}" } }
    
    credentials = options[:database_config]
    if options[:database_connection_string]
      credentials = DatabaseHelper.get_database_credentials(
        options[:database_type],
        options[:database_connection_string]
      )
    end

    @target_connection = CachedConnection.new(
      options[:database_type],
      credentials,
      options[:cache] || {}
    )
  end

  def query(params = {})
    validate_query_params(params)

    tenants = params[:tenants]
    flags = params[:flags]
    metadata = params[:metadata]
    metadata = metadata.transform_keys(&:to_sym)
    filters = params[:filters]

    # TODO: Add support for multiple tenants
    if tenants&.any?
      @target_connection.tenant_ids = TenantUtils.extract_tenant_ids(tenants)
    end

    response_metadata = {}

    unless metadata[:task]
      return { error: "Missing task.", status: "error", data: {} }
    end

    tenant_flags = nil

    begin
      # If the task requires flags to be synthesized from tenants
      if FLAG_TASKS.include?(metadata[:task]) &&
         tenants&.first != ALL_TENANTS &&
         tenants&.first != SINGLE_TENANT
         
        response = post_quill('tenant-mapped-flags', {
          reportId: metadata[:reportId] || metadata[:dashboardItemId],
          dashboardName: metadata[:name],
          tenants: tenants,
          flags: flags
        })

        return {
          status: "error",
          error: response[:error],
          data: response[:metadata] || {}
        } if response[:error]

        flag_query_results = run_queries(response[:queries], @target_connection.database_type)
        tenant_flags = Set.new(
          flag_query_results[:queryResults].flat_map do |result|
            result[:rows].map { |row| row['quill_flag'] }
          end
        )
      elsif tenants&.first == SINGLE_TENANT && flags
        unless flags.first.is_a?(String)
          return {
            status: "error",
            error: "SINGLE_TENANT only supports string[] for the flags parameter",
            data: {}
          }
        end
        tenant_flags = Set.new(flags)
      end

      pre_query_results = if metadata[:preQueries]
                            run_queries(
                              metadata[:preQueries],
                              @target_connection.database_type,
                              metadata[:databaseType],
                              metadata[:runQueryConfig]
                            )
                          else
                            {}
                          end

      if metadata.dig(:runQueryConfig, "overridePost")
        return {
          data: { queryResults: pre_query_results },
          status: "success"
        }
      end

      response = post_quill(metadata[:task], {
        **metadata,
        sdk_filters: filters&.map { |filter| FilterUtils.convert_custom_filter(filter) },
        **pre_query_results,
        tenants: tenants,
        flags: tenant_flags ? tenant_flags.to_a : nil,
        viewQuery: metadata[:preQueries]&.first
      })

      return {
        status: "error",
        error: response[:error],
        data: response[:metadata] || {}
      } if response[:error]

      response_metadata = response[:metadata] if response[:metadata]

      results = run_queries(
        response[:queries],
        @target_connection.database_type,
        metadata[:database_type],
        response_metadata.dig(:runQueryConfig)
      )

      if results[:mappedArray] && response_metadata.dig(:runQueryConfig, "arrayToMap")
        array_to_map = response_metadata.dig(:runQueryConfig, "arrayToMap")
        results[:mappedArray].each_with_index do |array, index|
          response_metadata[array_to_map[:arrayName]][index][array_to_map[:field]] = array
        end
        results.delete(:mappedArray)
      end

      if results[:queryResults]&.size == 1
        query_results = results[:queryResults].first
        response_metadata[:rows] = query_results[:rows] if query_results[:rows]
        response_metadata[:fields] = query_results[:fields] if query_results[:fields]
      end

      {
        data: response_metadata,
        queries: results,
        status: "success"
      }
    rescue StandardError => e
      if metadata[:task] == "update-view"
        post_quill("set-broken-view", {
          table: metadata[:name],
          clientId: metadata[:clientId],
          error: e.message
        })
      end

      {
        status: "error",
        error: e.message,
        data: response_metadata || {}
      }
    end
  end

  private

  def validate_options(options)
    if !options[:private_key]
      raise ArgumentError, "Private key is required"
    end
    if !options[:database_type]
      raise ArgumentError, "Database type is required"
    end
    if !DatabaseType.valid?(options[:database_type])
      raise ArgumentError, "Invalid database type"
    end
    if !options[:database_connection_string] && !options[:database_config]
      raise ArgumentError, "Either database_connection_string or database_config is required"
    end
  end

  def validate_query_params(params)
    if !params[:tenants]
      raise ArgumentError, "tenants is required"
    end
    if !params[:metadata]
      raise ArgumentError, "metadata is required"
    end
  end

  def run_queries(queries, pk_database_type, database_type = nil, run_query_config = nil)
    return { queryResults: [] } unless queries
  
    if database_type && database_type.downcase != pk_database_type.downcase
      return {
        dbMismatched: true,
        backendDatabaseType: pk_database_type,
        queryResults: []
      }
    end
  
    results = {}

    run_query_config = run_query_config.transform_keys(&:to_sym) if run_query_config
  
    if run_query_config&.dig(:arrayToMap)
      mapped_array = RunQueryProcesses.map_queries(queries, @target_connection)
      results[:queryResults] = []
      results[:mappedArray] = mapped_array
  
    elsif run_query_config&.dig(:getColumns)
      query_result = @target_connection.query("#{queries[0].gsub(/;/, '')} limit 1000")
      columns = query_result[:fields].map do |field|
        {
          fieldType: Schema.convert_type_to_postgres(field[:dataTypeID]),
          name: field[:name],
          displayName: field[:name],
          isVisible: true,
          field: field[:name]
        }
      end
      results[:columns] = columns  
    elsif run_query_config&.dig(:getColumnsForSchema)
      query_results = queries.map do |table|
        if table[:viewQuery].nil? || (!table[:isSelectStar] && !table[:customFieldInfo])
          table
        else
          limit = run_query_config[:limitBy] ? " limit #{run_query_config[:limitBy]}" : ""
          begin
            query_result = @target_connection.query("#{table[:viewQuery].gsub(/;/, '')} #{limit}")
            columns = query_result[:fields].map do |field|
              {
                fieldType: Schema.convert_type_to_postgres(field[:dataTypeID]),
                name: field[:name],
                displayName: field[:name],
                isVisible: true,
                field: field[:name]
              }
            end
            table.merge(columns: columns, rows: query_result[:rows])
          rescue StandardError => e
            table.merge(error: "Error fetching columns: #{e.message}")
          end
        end
      end
  
      results[:queryResults] = query_results
  
      if run_query_config&.dig(:fieldsToRemove)
        results[:queryResults] = query_results.map do |table|
          removed_columns = table[:columns]&.reject { |column| run_query_config[:fieldsToRemove]&.include?(column[:name]) }
          table.merge(columns: removed_columns)
        end
      end
  
    elsif run_query_config&.dig(:getTables)
      schema_names = run_query_config[:schemaNames] || run_query_config[:schema]
      tables_info = DatabaseHelper.get_tables_by_schema_by_database(@target_connection.database_type, @target_connection.pool, schema_names)
      schema_info = DatabaseHelper.get_column_info_by_schema_by_database(@target_connection.database_type, @target_connection.pool, run_query_config[:schema], tables_info)
      return schema_info
  
    else
      modified_queries = queries
      if run_query_config&.dig(:limitThousand)
        modified_queries = queries.map { |q| "#{q.gsub(/;/, '')} limit 1000;" }
      elsif run_query_config&.dig(:limitBy)
        modified_queries = queries.map { |q| "#{q.gsub(/;/, '')} limit #{run_query_config[:limitBy]};" }
      end
  
      query_results = modified_queries.map { |query| @target_connection.query(query) }
      results[:queryResults] = query_results
  
      if run_query_config&.dig(:fieldsToRemove)
        results[:queryResults] = query_results.map do |result|
          RunQueryProcesses.remove_fields(result, run_query_config[:fieldsToRemove])
        end
      end

      if run_query_config&.dig(:convertDatatypes)
        results = query_results.map do |result|
          {
            fields: result[:fields].map do |field|
              field.merge(
                fieldType: Schema.convert_type_to_postgres(field[:dataTypeID]),
                isVisible: true,
                field: field[:name],
                displayName: field[:name],
                name: field[:name]
              )
            end,
            rows: result[:rows]
          }
        end
      end
    end

    results
  end

  def post_quill(path, payload)
    uri = URI("#{@base_url}/sdk/#{path}")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = uri.scheme == 'https'
    request = Net::HTTP::Post.new(uri)
    request['Authorization'] = @config[:headers]["Authorization"]
    request['Content-Type'] = 'application/json'
    request.body = payload.to_json

    response = http.request(request)
    
    if !response.is_a?(Net::HTTPSuccess)
      body = JSON.parse(response.body, symbolize_names: true)
      raise QuillAPIError.new("#{body[:error]}")
    end
    
    JSON.parse(response.body, symbolize_names: true)
  rescue JSON::ParserError => e
    raise QuillAPIError.new("Invalid JSON response: #{e.message}")
  rescue => e
    raise QuillAPIError.new(e.message)
  end
  
  def async_dispose
    close
  end

  def close
    @target_connection&.close
  end
end