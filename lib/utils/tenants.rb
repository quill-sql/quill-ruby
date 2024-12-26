module TenantUtils
  def self.extract_tenant_ids(tenants)
    if tenants[0].is_a?(String) || tenants[0].is_a?(Numeric)
      tenants
    elsif tenants[0].is_a?(Hash) && tenants[0].key?('tenant_ids')
      tenants[0]['tenant_ids']
    else
      raise 'Invalid format for tenants'
    end
  end

  def self.extract_tenant_field(tenants, dashboard_owner)
    if tenants[0].is_a?(String) || tenants[0].is_a?(Numeric)
      dashboard_owner
    elsif tenants[0].is_a?(Hash) && tenants[0].key?('tenant_field')
      tenants[0]['tenant_field']
    else
      raise 'Invalid format for tenants'
    end
  end
end