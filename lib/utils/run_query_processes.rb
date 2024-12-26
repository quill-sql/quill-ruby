class TableSchemaInfo
  attr_accessor :field_type, :name, :display_name, :is_visible

  def initialize(field_type:, name:, display_name:, is_visible:)
    @field_type = field_type
    @name = name
    @display_name = display_name
    @is_visible = is_visible
  end
end

module RunQueryProcesses
  def self.remove_fields(query_results, fields_to_remove)
    fields = query_results[:fields].reject { |field| fields_to_remove.include?(field[:name]) }
    rows = query_results[:rows].map do |row|
      fields_to_remove.each { |field| row.delete(field) }
      row
    end
    { fields: fields, rows: rows }
  end

  def self.map_queries(queries, target_connection)
    queries.map { |query| target_connection.query(query).rows }
  end
end