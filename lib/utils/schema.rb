require_relative '../assets/pg_types'

module Schema
  def self.convert_type_to_postgres(data_type_id)
    type = PG_TYPES.find { |t| data_type_id == t[:oid] }&.dig(:typname)
    type || 'varchar'
  end
end