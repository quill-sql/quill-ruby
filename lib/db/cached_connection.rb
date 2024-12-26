require 'redis'
require 'json'
require_relative 'db_helper'

class CachedConnection
  DEFAULT_CACHE_TTL = 24 * 60 * 60 # 24 hours in seconds

  attr_reader :database_type, :pool, :ttl, :cache
  attr_reader :closed
  attr_writer :tenant_ids

  def initialize(database_type, config, cache_config = {})
    @database_type = database_type
    @pool = DatabaseHelper.connect_to_database(database_type, config)
    @tenant_ids = nil
    @ttl = cache_config[:ttl] || DEFAULT_CACHE_TTL
    @cache = get_cache(cache_config)
    @closed = false
  end

  def query(text)
    raise "Connection is closed" if @closed

    if @cache.nil?
      return DatabaseHelper.run_query_by_database(@database_type, @pool, text)
    end

    key = "#{@tenant_ids}:#{text}"
    cached_result = @cache.get(key)

    if cached_result
      JSON.parse(cached_result)
    else
      new_result = DatabaseHelper.run_query_by_database(@database_type, @pool, text)
      new_result_string = JSON.generate(new_result)
      @cache.set(key, new_result_string, "EX", DEFAULT_CACHE_TTL)
      new_result
    end
  rescue StandardError => e
    raise StandardError, e.message
  end

  def get_pool
    @pool
  end

  def close
    DatabaseHelper.disconnect_from_database(@database_type, @pool)
    @closed = true
  end

  private

  def get_cache(config)
    return nil unless config[:cacheType]&.match?(/^redis(s)?$/)

    redis_url = "#{config[:cacheType]}://#{config[:username]}:#{config[:password]}@#{config[:host]}:#{config[:port]}"
    Redis.new(url: redis_url)
  end
end